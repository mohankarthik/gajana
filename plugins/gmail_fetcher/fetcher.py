import os
import os.path
import json
import logging
from datetime import datetime, timedelta
import base64
import io

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

class GmailFetcher:
    def __init__(self, drive_service):
        self.drive_service = drive_service # Reuse Gajana's service account for Drive
        self.gmail_service = self._get_gmail_service()
        self.settings = self._load_settings()

    def _load_settings(self):
        settings_path = os.path.join(os.path.dirname(__file__), "settings.json")
        try:
            with open(settings_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load settings.json: {e}")
            return None

    def _get_gmail_service(self):
        creds = None
        # Put credentials in the root secrets/ folder
        token_path = os.path.join(os.getcwd(), "secrets", "gmail_token.json")
        creds_path = os.path.join(os.getcwd(), "secrets", "gmail_credentials.json")
        
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(creds_path):
                    logger.error(f"Missing {creds_path}. Please download an OAuth 2.0 Desktop Client ID JSON from GCP and place it there.")
                    return None
                flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
                # Headless server support: print URL, user opens locally, then redirects to localhost
                logger.info("\n" + "="*80)
                logger.info("HEADLESS AUTHENTICATION REQUIRED")
                logger.info("1. Copy the URL printed below and open it in a browser on your local computer.")
                logger.info("2. Complete the Google login.")
                logger.info("3. You will be redirected to a 'This site can't be reached' page (localhost:8080).")
                logger.info("4. Copy the ENTIRE localhost URL from your browser's address bar.")
                logger.info("5. Open a NEW terminal on this server and run: curl \"<PASTE_URL_HERE>\"")
                logger.info("="*80 + "\n")
                creds = flow.run_local_server(port=0, open_browser=False)
            
            with open(token_path, 'w') as token:
                token.write(creds.to_json())

        try:
            service = build('gmail', 'v1', credentials=creds)
            return service
        except HttpError as error:
            logger.error(f'An error occurred setting up Gmail service: {error}')
            return None

    def fetch_and_upload(self, days_back=7):
        if not self.gmail_service or not self.settings:
            return

        folder_id = self.settings.get("gajana_folder_id")
        configs = self.settings.get("configs", [])
        
        # Calculate date for search query
        date_from = (datetime.now() - timedelta(days=days_back)).strftime('%Y/%m/%d')
        
        for config in configs:
            subject = config["subject"]
            prefix = config["prefix"]
            sender = config.get("from")
            
            query = ""
            if sender:
                query += f"from:{sender} "
            query += f"subject:\"{subject}\" has:attachment -in:trash after:{date_from}"
            
            logger.info(f"Searching Gmail for: {query}")
            
            try:
                results = self.gmail_service.users().messages().list(userId='me', q=query).execute()
                messages = results.get('messages', [])
                
                if not messages:
                    continue
                    
                for message in messages:
                    msg = self.gmail_service.users().messages().get(userId='me', id=message['id']).execute()
                    
                    # Get internal date
                    internal_date = int(msg['internalDate']) / 1000.0
                    msg_date = datetime.fromtimestamp(internal_date)
                    
                    # Bank statements usually arrive early in the following month. 
                    # If received before the 15th, assign it to the previous month.
                    if msg_date.day < 15:
                        # Subtract enough days to safely land in the previous month
                        statement_date = msg_date - timedelta(days=20)
                        date_str = statement_date.strftime("%Y-%m")
                    else:
                        date_str = msg_date.strftime("%Y-%m")
                        
                    file_name = f"{prefix}-{date_str}.pdf"
                    
                    # Check if file already exists in Drive. If so, append _copyX so we NEVER lose data.
                    base_file_name = file_name
                    copy_idx = 1
                    while True:
                        query_drive = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
                        res = self.drive_service.files().list(q=query_drive, spaces="drive").execute()
                        if not res.get('files', []):
                            break
                        # Append copy suffix
                        name_part, ext_part = base_file_name.rsplit('.', 1)
                        file_name = f"{name_part}_copy{copy_idx}.{ext_part}"
                        copy_idx += 1
                        
                    # Find PDF attachment
                    parts = msg['payload'].get('parts', [])
                    for part in parts:
                        if part['filename'] and part['filename'].lower().endswith('.pdf'):
                            if 'data' in part['body']:
                                data = part['body']['data']
                            else:
                                att_id = part['body']['attachmentId']
                                att = self.gmail_service.users().messages().attachments().get(userId='me', messageId=msg['id'], id=att_id).execute()
                                data = att['data']
                                
                            file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                            
                            # Upload to Drive
                            file_metadata = {
                                'name': file_name,
                                'parents': [folder_id]
                            }
                            media = MediaIoBaseUpload(io.BytesIO(file_data), mimetype='application/pdf', resumable=True)
                            
                            logger.info(f"Uploading {file_name} to Drive...")
                            uploaded_file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                            logger.info(f"Successfully created {file_name} with ID: {uploaded_file.get('id')}")
                            
                            # Trash the email so we don't process it again
                            self.gmail_service.users().messages().trash(userId='me', id=msg['id']).execute()
                            logger.info(f"Trashed email {msg['id']} to prevent reprocessing.")
                            break # Found the attachment
                            
            except HttpError as error:
                logger.error(f'An error occurred searching Gmail: {error}')

def run_plugin(drive_service, days_back=7):
    logger.info("Starting Gmail Fetcher Plugin...")
    fetcher = GmailFetcher(drive_service)
    fetcher.fetch_and_upload(days_back=days_back)
    logger.info("Gmail Fetcher Plugin finished.")
