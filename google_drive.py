# local imports
import io
import os
import requests

# Google API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# Google Drive class
class GoogleDrive:
    def __init__(self):
        # delete token.json before changing these
        self.scopes = [
            # 'https://www.googleapis.com/auth/drive.metadata.readonly',
            'https://www.googleapis.com/auth/drive.readonly'
        ]
        self.creds = None
        self.credentials()
        self.connect()
    
    def credentials(self):
        # store credentials (user access and refresh tokens)
        if os.path.exists('token.json'):
            self.creds = Credentials.from_authorized_user_file('token.json', self.scopes)
        # if no (valid) credentials available, let user log in
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', self.scopes)
                self.creds = flow.run_local_server()  # port MUST match redirect URI in Google App
            # save credentials for the next run
            with open('token.json', 'w') as token:
                token.write(self.creds.to_json())
                
    def connect(self):
        # attempt to connect to the API
        try:
            self.service = build('drive', 'v2', credentials=self.creds) # used to retrieve all revision author and to export google workspace 
            # self.service = build('gmail', 'v1', credentials=self.creds)  # use later for gmail...
        except HttpError as error:
            print(f'An error occurred: {error}')
            
    def id_get(self, i):
        r = self.service.files().get(fileId=i).execute()
        
        return r
            
    def id_search(self, values, term='name', operator='=', ftype='file', ignore_trashed=True):
        q = f'{term} {operator} "{values}" '
        if ftype == 'folder':
            q += 'and mimeType = "application/vnd.google-apps.folder" '
        elif ftype == 'json':
            q += 'and mimeType = "application/json" '
        if ignore_trashed:
            q += 'and trashed = false'
        l = self.service.files().list(q=q).execute()
        
        return l['files']
            
    def folder_contents_v3(self, i, ignore_trashed=True):
        q = f'"{i}" in parents '
        if ignore_trashed:
            q += 'and trashed = false '
        l = self.service.files().list(q=q, fields='files(id,name,mimeType,createdTime,modifiedTime,lastModifyingUser(displayName,emailAddress))').execute()
        
        return l['files']
    
    def folder_contents_v2(self, i, ignore_trashed=True):
        q = f'"{i}" in parents '
        if ignore_trashed:
            q += 'and trashed = false '
        l = self.service.files().list(q=q, projection='FULL').execute()
        
        return l['items']
            
    def get_shortcut_target_v3(self, shortcut_id):
        # Récupère les informations sur la cible du shortcut
        file = self.service.files().get(fileId=shortcut_id, fields="shortcutDetails/targetId", supportsAllDrives=True).execute()
        target_id = file.get('shortcutDetails', {}).get('targetId', None)
        if target_id:
            # Récupère le fichier cible
            try:
                target_file = self.service.files().get(fileId=target_id, fields='id,name,mimeType,createdTime,modifiedTime,lastModifyingUser(displayName,emailAddress)', supportsAllDrives=True).execute()
                return target_file
            except HttpError as error:
                print(f"Erreur lors de la récupération du fichier cible : {error}")
                return None
        else:
            return None
        
    def get_shortcut_target_v2(self, shortcut_id):
        # Récupère les informations sur la cible du shortcut
        file = self.service.files().get(fileId=shortcut_id, fields="shortcutDetails/targetId", supportsAllDrives=True).execute()
        target_id = file.get('shortcutDetails', {}).get('targetId', None)
        if target_id:
            # Récupère le fichier cible
            try:
                target_file = self.service.files().get(fileId=target_id, projection='FULL', supportsAllDrives=True).execute()
                return target_file
            except HttpError as error:
                print(f"Erreur lors de la récupération du fichier cible : {error}")
                return None
        else:
            return None

    def get_revisions_v3(self, i):
        try:
            r = self.service.revisions().list(fileId=i).execute()
        
            return r['revisions']
        
        except:
            return
        
    def get_revisions_v2(self, i):
        try:
            r = self.service.revisions().list(fileId=i).execute()

            return r['items']
        
        except Exception as exception:
            return
        
    def qry_fields(self, i, r=None, fields=['parents']):
        if r is None:
            p = self.service.files().get(fileId=i, fields=','.join(fields), supportsAllDrives=True).execute()
        else:
            p = self.service.revisions().get(fileId=i, revisionId=r, fields=','.join(fields), supportsAllDrives=True).execute()
        
        return {f: p[f] for f in fields}
    
    def stream_file_v3(self, f, out='stream', verbose=False):
        mime = f['type']
        file_id = f['id']
        rev_id = f['rid']

        export_map = {
            'application/vnd.google-apps.document': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',  # .docx
            'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',     # .xlsx
            'application/vnd.google-apps.presentation': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',  # .pptx
        }
        
        if rev_id is None:
            if mime in export_map:
                request = self.service.files().export(fileId=file_id, mimeType=export_map[mime])
            else:
                request = self.service.files().get_media(fileId=file_id)
        else:
            if mime in export_map:
                request = self.service.revisions().get_media(fileId=file_id)
            else:
                request = self.service.revisions().get_media(fileId=file_id, revisionId=rev_id)
        
        if out in ['stream', 'str']:
            stream = io.BytesIO()
        else:
            stream = io.FileIO(out, mode='w')
        downloader = MediaIoBaseDownload(stream, request)

        try:
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if verbose:
                    print(f'Download {int(status.progress() * 100)}%')
            if verbose:
                print(f'Size {status.total_size / 1024 / 1024:.2f}MB')
        except Exception as exception:
            stream.close()

            if os.path.exists(out):
                os.remove(out)

            print(f"\t\tDownload failed, discarded: {str(exception)}")

        if out in ['str']:
            return stream.getvalue()
        else:
            return stream

    def stream_file_v2(self, f, out='stream', verbose=False):
        mime = f['type']
        file_id = f['id']
        rev_id = f['rid']

        export_map = {
            'application/vnd.google-apps.document': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',  # .docx
            'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',     # .xlsx
            'application/vnd.google-apps.presentation': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',  # .pptx
        }

        export_native_map = {
            'application/vnd.google-apps.document': 'docx',
            'application/vnd.google-apps.spreadsheet': 'xlsx',
            'application/vnd.google-apps.presentation': 'pptx',
            'application/vnd.google-apps.drawing': 'png',
        }

        format = export_native_map.get(mime)

        if format:
            if rev_id:
                export_links = self.service.revisions().get(fileId=file_id, revisionId=rev_id, fields='exportLinks').execute().get('exportLinks')
                url = export_links[export_map[mime]]

                url += f'&revision={rev_id}'
            else:
                export_links = self.service.files().get(fileId=file_id, fields='exportLinks').execute().get('exportLinks')
                url = export_links[export_map[mime]]
        else:
            if rev_id:
                url = self.service.revisions().get(fileId=file_id, revisionId=rev_id, fields='downloadUrl').execute().get('downloadUrl')
            else:
                url = self.service.files().get(fileId=file_id, fields='downloadUrl', supportsAllDrives=True).execute().get('downloadUrl')

        headers = {'Authorization': f'Bearer {self.creds.token}'}
        resp = requests.get(url, headers=headers, stream=True)
        resp.raise_for_status()

        if out in ['stream', 'str']:
            stream = io.BytesIO()
        else:
            stream = io.FileIO(out, mode='w')

        # Lecture par morceaux
        for chunk in resp.iter_content(chunk_size=32768):
            if not chunk:
                continue
            stream.write(chunk)
            if verbose:
                print(f'Downloaded {len(chunk)} bytes')

        if out in ['str']:
            return stream.getvalue()
        else:
            return stream