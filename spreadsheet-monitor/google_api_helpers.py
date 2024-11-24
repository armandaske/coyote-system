from flask import url_for
from google.cloud import secretmanager
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from json import loads
from os import getenv

# If modifying these SCOPES, delete the stored refresh token.
SCOPES = ['https://www.googleapis.com/auth/drive', 
          'https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/calendar']


def get_creds(firestore_db):
    creds = None
    refresh_token_doc = firestore_db.collection('app').document('refresh_token').get()  # Get the stored refresh token from Firestore

    if refresh_token_doc.exists:
        # If the refresh token exists, create Credentials from it
        creds_dict = loads(refresh_token_doc.to_dict()['refresh_token'])
        creds = Credentials.from_authorized_user_info(creds_dict, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print('refreshing token')
            creds.refresh(Request())
            # Save the refresh token in Firestore
            firestore_db.collection('app').document('refresh_token').set({
                'refresh_token': creds.to_json()}, merge=True)
        else:
            creds = None
    return creds

def get_oauth2_flow():
    print('trying to pop the oauth2 flow screen')
    # Initialize the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Replace these with the name of your secret.
    project_id = getenv('PROJECT_ID')
    secret_id = getenv('SECRET_ID')

    # Build the resource name of the secret.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    # Access the secret.
    response = client.access_secret_version(request={"name": name})

    # Parse the secret payload as JSON.
    client_secrets = loads(response.payload.data.decode("UTF-8"))

    return Flow.from_client_config(client_secrets, SCOPES, redirect_uri=url_for('oauth2callback', _external=True))
