from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import os
import pickle
from time import sleep
import pandas as pd

# Scope needed to read/write spreadsheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Path to your OAuth credentials JSON file
OAUTH_CREDENTIALS_FILE = 'C:/Users/Dell-G3/Downloads/client_secret_640474142702-5odg79ueaoe9spe8f7k3plbvlme44vnt.apps.googleusercontent.com.json'

CREDENTIALS_FILE = 'token.json'

# Check if token already exists
if os.path.exists(CREDENTIALS_FILE):
    with open(CREDENTIALS_FILE, 'rb') as token:
        creds = pickle.load(token)
else:
    # If no valid credentials, go through OAuth flow
    flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CREDENTIALS_FILE, SCOPES)
    creds = flow.run_local_server(port=0, prompt='select_account')

    # Save credentials to a file for the next run
    with open(CREDENTIALS_FILE, 'wb') as token:
        pickle.dump(creds, token)

# Build the Sheets API service
service = build('sheets', 'v4', credentials=creds)

# List of spreadsheet IDs

sheet_ids_df=pd.read_csv('C:/Users/Dell-G3/.spyder-py3/coyote-system/tests/sheets_ids.csv',header=None)
sheet_ids = sheet_ids_df.iloc[:, 0].tolist()
#print(sheet_ids)

#Define the value to write
body = {
    'values': [[0]]  # Writing 0 into one cell
}
#print(len(sheet_ids))
for sheet_id in sheet_ids:
    sleep(6)
    try:
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='P250',                # Target cell
            valueInputOption='RAW',     # You can use 'USER_ENTERED' if you want it interpreted like a user input
            body=body
        ).execute()
        print(f"{sheet_id}: P250 updated successfully.")
    except Exception as e:
        print(f"Failed to update {sheet_id}: {e}")
