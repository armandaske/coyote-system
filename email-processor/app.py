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
    #'labelIds': ['UNREAD'], #UNREAD, INBOX
    'topicName': f'projects/{PROJECT_ID}/topics/{TOPIC_ID}',
    #'labelFilterBehavior': 'INCLUDE'
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


def pubsub_handler(gmail_service, drive_service, sheets_service, payload):
    print(f"Received message: {payload}")
    locked2 = False
    try:
        #1.  Load previous historyId
        doc = firestore_db.collection('email_history').document('history_id').get()
        doc_data = doc.to_dict()
        previous_history_id_raw = doc_data.get("historyId")
        previous_history_id = str(int(previous_history_id_raw.strip()))

        # 2. Use users.history.list to get new messages
        response = gmail_service.users().history().list(
            userId='me',
            startHistoryId=previous_history_id,
            historyTypes=['messageAdded', 'labelAdded', 'labelRemoved']
        ).execute()


        #print(f"Response from Gmail API: {response}")
        # 3. Process new messages
        if 'history' in response:
            email_scraper_main(drive_service, sheets_service, gmail_service)
            print('finished email_scraper routine')

        # 4. Update historyId in Firestore
        new_history_id = response.get('historyId')
        if new_history_id:
            firestore_db.collection('email_history').document('history_id').set({
                'historyId': new_history_id
            })

        doc = firestore_db.collection('locks').document('pubsub-lock').get()
        locked2 = doc.to_dict().get('locked2')

    except Exception as e:
        print(f"Error in pubsub_handler: {e}")

    finally:
        release_lock()
        if locked2:
            payload = {'message': 'new endpoint call from my app'}
            headers = {'Content-Type': 'application/json'}
            requests.post(URL + '/pubsub-endpoint', json=payload, headers=headers)

        return 'released lock complete'



def release_lock():
    lock_ref = firestore_db.collection('locks').document('pubsub-lock')
    lock_ref.set({'locked': False, 'locked2': False})


@app.route('/reset-watch')
def reset_watch():
    gmail_service = build('gmail', 'v1', credentials=g.creds)
    watch = gmail_service.users().watch(userId='me', body=watch_request).execute()

    # Save the historyId to Firestore
    firestore_db.collection('email_history').document('history_id').set({
        'historyId': watch.get('historyId')
    })

    print(f"Watch started. {watch}")
    return 'Watch method reset successfully', 200



@app.route('/stop-watch')
def stop_watch():
    # Execute the watch request
    gmail_service = build('gmail', 'v1', credentials=g.creds)
    
    # Detiene el modo watch de la API de Gmail
    stop = gmail_service.users().stop(userId='me').execute()
    print(stop)
    return 'Stop watch method done successfully', 200




@app.route('/ping-endpoint')
def ping_endpoint():
    url = 'https://starlit-complex-410801.uc.r.appspot.com/pubsub-endpoint'

    # Hardcoded payload mimicking a Pub/Sub push
    payload = {
        "message": {
            "data": "dGVzdA=="  # "test" in base64
        }
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        return f"Ping sent. Response status: {response.status_code}, body: {response.text}", 200
    except Exception as e:
        return f"Error during ping: {str(e)}", 500
