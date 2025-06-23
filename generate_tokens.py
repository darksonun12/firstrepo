#!/usr/bin/env python
import os
import json
import pickle
import webbrowser
import sys
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import time

# Print ASCII art banner for better user experience
print("""\n==========================================================
   YouTube Multi-Account Authentication Token Generator
==========================================================

This script will help you authenticate with multiple YouTube channels
and save their access tokens for future use without reauthorization.

You will need to:
1. Have 5 different Gmail accounts ready
2. Authenticate 2 YouTube channels per Gmail account
3. Sign out between each Gmail account when prompted

If you encounter any errors, simply restart the script.
""")

# Path configurations
CLIENT_SECRETS_DIR = os.path.abspath(os.path.dirname(__file__))
TOKEN_DIR = os.path.join(CLIENT_SECRETS_DIR, "tokens")
CLIENT_FILES = ["gmail3.json"]

# Create tokens directory if it doesn't exist
if not os.path.exists(TOKEN_DIR):
    os.makedirs(TOKEN_DIR)
    print(f"Created tokens directory at {TOKEN_DIR}")

# YouTube API scopes needed for uploading videos
SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.force-ssl"
]

def get_authenticated_service(client_secret_file, account_id=0):
    """
    Authenticate and build the YouTube API service.
    Will handle 2 YouTube accounts per client secret file.
    """
    # Create a unique token file path for this account
    token_pickle = os.path.join(TOKEN_DIR, f"temp_token_{account_id}.pickle")
    
    # For each authentication, we want to start fresh
    creds = None
    
    # If credentials are invalid or don't exist, authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            client_secret_path = os.path.join(CLIENT_SECRETS_DIR, client_secret_file)
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
            
            print("\n[WARNING] A browser window will open for authentication.")
            print("[WARNING] Please select the correct YouTube channel when prompted.")
            input("Press Enter to open the authentication window...")
            
            # Run the OAuth flow - this will open a browser window
            creds = flow.run_local_server(port=0)
        
        # Save credentials for next run
        with open(token_pickle, 'wb') as token:
            pickle.dump(creds, token)
    
    # Build the YouTube API service
    service = build('youtube', 'v3', credentials=creds)
    return service, creds

def get_channel_info(service):
    """Get the YouTube channel name and ID."""
    request = service.channels().list(
        part="snippet,contentDetails",
        mine=True
    )
    response = request.execute()
    
    if 'items' in response and len(response['items']) > 0:
        channel_info = response['items'][0]['snippet']
        channel_id = response['items'][0]['id']
        channel_name = channel_info['title']
        return channel_name, channel_id
    else:
        return "UnknownChannel", "UnknownID"

def save_channel_token(creds, channel_name, channel_id):
    """Save the credentials for a specific channel."""
    # Replace any characters that aren't suitable for filenames
    safe_channel_name = ''.join(c if c.isalnum() or c in ' _-' else '_' for c in channel_name)
    
    # Create token file path with channel name
    token_file = os.path.join(TOKEN_DIR, f"{safe_channel_name}_{channel_id}.pickle")
    
    # Save the credentials
    with open(token_file, 'wb') as token:
        pickle.dump(creds, token)
    
    print(f"Saved token for channel: {channel_name} (ID: {channel_id})")
    return token_file

def main():
    """
    Main function to authenticate with multiple YouTube accounts and save their tokens.
    """
    print("Starting YouTube authentication process for multiple accounts...")
    
    account_counter = 0
    for client_file in CLIENT_FILES:
        print(f"\n===== Processing client secrets file: {client_file} =====")
        print("\n[IMPORTANT] You need to authenticate with a NEW Gmail account for this client file.")
        print("[IMPORTANT] Please make sure you're signed out of your previous Gmail account in your browser.")
        input(f"\nPress Enter when you're ready to authenticate 2 YouTube channels for {client_file}...")
        
        # Each client file can have 2 YouTube accounts
        for i in range(2):
            print(f"\nAuthenticating YouTube account #{i+1} for {client_file}...")
            account_id = account_counter
            
            try:
                # Get authenticated service
                service, creds = get_authenticated_service(client_file, account_id)
                
                # Get channel info
                channel_name, channel_id = get_channel_info(service)
                
                # Save token with channel name
                token_path = save_channel_token(creds, channel_name, channel_id)
                print(f"Successfully saved token at: {token_path}")
                
                # Remove the temporary token file
                temp_token = os.path.join(TOKEN_DIR, f"temp_token_{account_id}.pickle")
                if os.path.exists(temp_token):
                    os.remove(temp_token)
                
                # Wait a moment between authentications for the same client file
                if i == 0:
                    print("\nWaiting for 3 seconds before authenticating the second YouTube channel...")
                    time.sleep(3)
                    print("\n[IMPORTANT] Now authenticating the SECOND YouTube channel for the SAME Gmail account.")
                    print("[IMPORTANT] Please use the SAME Gmail account but select a DIFFERENT YouTube channel.")
                    input("Press Enter to continue...")
                
                account_counter += 1
                
            except Exception as e:
                print(f"Error authenticating account {account_counter}: {str(e)}")
        
        # After processing 2 YouTube channels for this client, prompt to change Gmail account
        if client_file != CLIENT_FILES[-1]:
            print("\n============================================================")
            print("[SUCCESS] FINISHED with this Gmail account and its 2 YouTube channels.")
            print("[IMPORTANT] You need to SIGN OUT of your current Gmail account now!")
            print("[IMPORTANT] Please follow these steps:")
            print("   1. Go to your browser")
            print("   2. Sign out of your current Gmail account")
            print("   3. Close all browser windows")
            print("   4. Return here to continue with the next Gmail account")
            input("\nPress Enter ONLY AFTER you've signed out of your current Gmail account...")
    
    print(f"\nAuthentication process complete!")
    print(f"Total accounts processed: {account_counter}")
    print(f"All tokens are saved in: {TOKEN_DIR}")
    print("\nYou can now use these tokens to upload videos to any of these channels without reauthorization.")

if __name__ == "__main__":
    main()
