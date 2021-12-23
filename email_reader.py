# import the required libraries
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os.path
import os
import base64
import json
from convert import to_json
from elasticsearch import Elasticsearch

def Create_Service(client_secret_file, api_name, api_version, *scopes, prefix=''):
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    
    cred = None
    working_dir = os.getcwd()
    token_dir = 'token files'
    pickle_file = f'token_{API_SERVICE_NAME}_{API_VERSION}{prefix}.pickle'

    if not os.path.exists(os.path.join(working_dir, token_dir)):
        os.mkdir(os.path.join(working_dir, token_dir))

    if os.path.exists(os.path.join(working_dir, token_dir, pickle_file)):
        with open(os.path.join(working_dir, token_dir, pickle_file), 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(os.path.join(working_dir, token_dir, pickle_file), 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        return service
    except Exception as e:
        print(e)
        print(f'Failed to create service instance for {API_SERVICE_NAME}')
        os.remove(os.path.join(working_dir, token_dir, pickle_file))
        return None

def construct_service(api_service):
    CLIENT_SERVICE_FILE = 'credentials.json'
    try:
        if api_service == 'gmail':
            API_NAME = 'gmail'
            API_VERSION = 'v1'
            SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
            return Create_Service(CLIENT_SERVICE_FILE, API_NAME, API_VERSION, SCOPES)

    except Exception as e:
        print(e)
        return None

def search_email(service, query_string, label_ids=[]):
    try:
        message_list_response = service.users().messages().list(
            userId='me',
            labelIds=label_ids,
            q=query_string
        ).execute()
        
        message_items = message_list_response.get('messages')
        nextPageToken = message_list_response.get('nextPageToken')

        while nextPageToken:
            message_list_response = service.users().messaage().list(
                userId='me',
                labelIds=label_ids,
                q=query_string,
                pageToken=nextPageToken
            ).execute()

            message_items.extend(message_list_response.get('messages'))
            nextPageToken = message_list_response.get('nextPageToken')
        
        return message_items
        
    except Exception as e:
        print(e)
        return None

def get_message_detail(service, message_id, format='metadata', metadata_headers=[]):
    try:
        message_detail = service.users().messages().get(
            userId='me',
            id=message_id,
            format=format,
            metadataHeaders=metadata_headers
        ).execute()

        return message_detail

    except Exception as e:
        print(e)
        return None

def post_data():
    gmail_service = construct_service('gmail')
    query_string = 'has:attachment subject:FWD: rawlogs'
    email_messages = search_email(gmail_service, query_string, ['INBOX'])

    for email_message in email_messages:
        messageId = email_message["threadId"]
        messageDetail = get_message_detail(
            gmail_service, email_message["id"], format="full", metadata_headers=["parts"]
        )
        messageDetailPayload = messageDetail.get("payload")

        for item in messageDetailPayload["headers"]:
            if item["name"] == "Subject":
                if item["value"]:
                    messageSubject = "{0} ({1})".format(item["value"], messageId)
                else:
                    messageSubject = "(No Subject) ({0})".format(messageId)

        if 'parts' in messageDetailPayload:
            for msgPayload in messageDetailPayload['parts']:
                mime_type = msgPayload['mimeType']
                file_name = msgPayload['filename']
                body = msgPayload['body']

                if 'attachmentId' in body:
                    attachment_id = body['attachmentId']

                    response = gmail_service.users().messages().attachments().get(
                        userId = 'me',
                        messageId = email_message["id"],
                        id = attachment_id
                    ).execute()
                    if file_name != 'noname':

                        file_data = base64.urlsafe_b64decode(response.get('data'))

                        new_arr = []
                        new_arr.append(file_name)
                        foreign_id = str(new_arr[0]).strip('rawlogs_').strip('.txt')
                        new_name = str(new_arr[0]).strip('.txt')

                        with open(f'{new_name}.json', 'wb') as f:
                            f.write(file_data)
                            f.close()
                        to_json(f'{new_name}.json')

                        with open(f'{new_name}.json', 'r') as f:
                            new_data = json.load(f)
                            es = Elasticsearch(host="localhost", port=9200)
                        
                            for i in new_data:
                                id_ = i['event_id']
                                data = {
                                    'alarm_id' : foreign_id,
                                    'data_pyload' : i['data_payload'],
                                    'binary_data' : i['binary_data']
                                }
                                
                                es.index(index="train_elastic_email",doc_type='doc_',id=id_,body=data)
                                print(f'Success input data with id {id_} alarm_id {foreign_id}')
                            f.close()
                            es.transport.close()
                            return json.dumps('Success Input All Data from Email Attachment')