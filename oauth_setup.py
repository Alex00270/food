#!/usr/bin/env python3
import os
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

OAUTH_CREDS = os.getenv('GOOGLE_OAUTH_CREDENTIALS_PATH', 'oauth_credentials.json')
TOKEN_PATH = os.getenv('GOOGLE_OAUTH_TOKEN_PATH', 'token.json')


def main():
    if not os.path.exists(OAUTH_CREDS):
        print(f"OAuth credentials file not found: {OAUTH_CREDS}")
        return

    flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CREDS, SCOPES)
    creds = flow.run_local_server(port=0)

    with open(TOKEN_PATH, 'w', encoding='utf-8') as token:
        token.write(creds.to_json())

    print(f"Token saved to {TOKEN_PATH}")


if __name__ == '__main__':
    main()
