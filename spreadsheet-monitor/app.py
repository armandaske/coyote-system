from flask import Flask, redirect, request, url_for
from google_api_helpers import get_creds, get_oauth2_flow
from googleapiclient.discovery import build
from main_function import main_function
from firebase_admin import credentials, firestore, initialize_app
from os import getenv
import gc

app = Flask(__name__)
app.secret_key = str(getenv('SECRET_KEY'))  # Make sure to set this environment variable

# Initialize Firebase
cred = credentials.ApplicationDefault() #I use ADC because i'm running it in the Google Cloud Environment so I can use the Application Default Credentials (ADC)
initialize_app(cred)
del cred
firestore_db = firestore.client() #my firebase database
gc.enable()

@app.route('/')
def home():
    creds = get_creds(firestore_db)
    if not creds or not creds.valid:
        return redirect(url_for('authorize'))
    # Build services here
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)
    calendar_service = build('calendar', 'v3', credentials=creds)
    # Run main function when i print i can see it in the sdk shell or the terminal, but if i return, it displays on the browser 
    try:
        main_function(drive_service, sheets_service, calendar_service, firestore_db)  # Pass the Firestore client to the main function
        return 'Main function executed successfully!', 200
    
    except Exception as e:
        return 'Error executing main function: ' + str(e), 500


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
    flow = get_oauth2_flow()
    flow.fetch_token(authorization_response=request.url)
    creds = flow.credentials
    # Save the refresh token in Firestore
    firestore_db.collection('app').document('refresh_token').set({
        'refresh_token': creds.to_json()}, merge=True)
    return redirect(url_for('home'))
