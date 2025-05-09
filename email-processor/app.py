from flask import Flask, redirect, request, url_for, g
from google_api_helpers import get_creds, get_oauth2_flow
from googleapiclient.discovery import build
from email_scraper import email_scraper_main
from firebase_admin import credentials, firestore, initialize_app
from os import getenv
import threading
import requests

app = Flask(__name__)
# Make sure to set this environment variable
app.secret_key = str(getenv('SECRET_KEY'))
PROJECT_ID = str(getenv('PROJECT_ID'))
TOPIC_ID = str(getenv('TOPIC_ID'))
URL = str(getenv('URL'))


# Initialize Firebase
# I use ADC because i'm running it in the Google Cloud Environment so I can use the Application Default Credentials (ADC)
cred = credentials.ApplicationDefault()
initialize_app(cred)
del cred
firestore_db = firestore.client()  # my firebase database
# Set up watch request
watch_request = {
    'labelIds': ['INBOX'],
    'topicName': f'projects/{PROJECT_ID}/topics/{TOPIC_ID}',
    'labelFilterBehavior': 'INCLUDE'
}


@app.before_request
def initialize_all():
    if request.path != '/authorize' and request.path != '/oauth2callback':
        print('getting credentials')
        g.creds = get_creds(firestore_db)
        if not g.creds or not g.creds.valid:
            print('going to authorize')
            return redirect(url_for('authorize'))


@app.route('/')
def home():
    # Run main function when i print i can see it in the sdk shell or the terminal, but if i return, it displays on the browser
    return 'Main function executed successfully!', 200


@app.route('/authorize')
def authorize():
    print('not creds in firestore (refresh_token.json)')
    flow = get_oauth2_flow()
    authorization_url = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent')
    return redirect(authorization_url[0])


@app.route('/oauth2callback')
def oauth2callback():
    print('oauth2callback')
    # print(session['origin_route'])
    flow = get_oauth2_flow()
    flow.fetch_token(authorization_response=request.url)
    creds = flow.credentials
    # Save the refresh token in Firestore
    firestore_db.collection('app').document('refresh_token').set({
        'refresh_token': creds.to_json()}, merge=True)
    return 'oauth2callback function executed successfully!', 200

# Endpoint for Pub/Sub messages


@app.route('/pubsub-endpoint', methods=['POST'])
def pubsub_endpoint():
    # Verify the request comes from Pub/Sub
    if request.headers.get('Content-Type') == 'application/json':
        data = request.get_json()
        if 'message' in data:
            payload = data['message']
            del data
            gmail_service = build('gmail', 'v1', credentials=g.creds)
            drive_service = build('drive', 'v3', credentials=g.creds)
            sheets_service = build('sheets', 'v4', credentials=g.creds)
            if acquire_lock('locked'):
                threading.Thread(target=pubsub_handler, args=(
                    gmail_service, drive_service, sheets_service, payload)).start()
                return 'pubsub done', 200
            elif acquire_lock('locked2'):
                print('other service is updating the database')
                return 'Other service is updating the database', 200
            else:
                print('let call go without processing it')
                return '', 204  # Let call go without processing it
        else:
            return 'No message in pubsub data', 400

    return 'Invalid request, Content-Type is not application/json', 400


def acquire_lock(field):
    transaction = firestore_db.transaction()
    doc = firestore_db.collection('locks').document('pubsub-lock')
    return transaction_callback(transaction, doc, field)


@firestore.transactional
def transaction_callback(transaction, doc, field):
    snapshot = doc.get(transaction=transaction)
    if not snapshot.exists or not snapshot.to_dict().get(field):
        transaction.update(doc, {field: True})
        print(f'{field} set to True')
        return True
    print(f'{field} was already True')
    return False

# @app.route('/pubsub-handler', methods=['POST'])


def pubsub_handler(gmail_service, drive_service, sheets_service, payload):
    print(f"Received message: {payload}")
    locked2 = False
    try:
        email_scraper_main(drive_service, sheets_service, gmail_service)
        print('finished email_scraper routine')
        doc = firestore_db.collection('locks').document('pubsub-lock')
        snapshot = doc.get()
        locked2 = snapshot.to_dict().get('locked2')
    finally:
        print('releasing lock')
        release_lock()
        if locked2:  # There's a pending change in gmail that occurred during the first processing. Will ping the pubsub-endpoint to run the email_scraper routine again
            payload = {'message': 'new endpoint call from my app'}
            headers = {'Content-Type': 'application/json'}
            requests.post(URL+'/pubsub-endpoint',
                          json=payload, headers=headers)
        return 'released lock complete'


def release_lock():
    lock_ref = firestore_db.collection('locks').document('pubsub-lock')
    lock_ref.set({'locked': False, 'locked2': False})


@app.route('/reset-watch')
def reset_watch():
    # Execute the watch request
    gmail_service = build('gmail', 'v1', credentials=g.creds)
    # Inicializa el modo watch de la API de Gmail así que solo debo llamarlo cada <7 días.
    watch = gmail_service.users().watch(userId='me', body=watch_request).execute()
    # La API deja de watchear y hacer push notifications hasta que el método stop sea llamado o después de 7 días
    print(watch)
    return 'Watch method reset successfully', 200
