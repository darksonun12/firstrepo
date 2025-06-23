#!/usr/bin/env python
import os
import json
import pickle
import glob
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Path configurations
TOKEN_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "tokens")
OUTPUT_FILE = os.path.join(os.path.abspath(os.path.dirname(__file__)), "channel_list.json")

def load_credentials(token_file):
    """Load credentials from a token file."""
    with open(token_file, 'rb') as token:
        credentials = pickle.load(token)
    
    # Refresh token if expired
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    
    return credentials

def get_channel_info(service):
    """Get the YouTube channel name and ID."""
    try:
        request = service.channels().list(
            part="snippet,contentDetails,statistics",
            mine=True
        )
        response = request.execute()
        
        if 'items' in response and len(response['items']) > 0:
            channel_info = response['items'][0]['snippet']
            channel_id = response['items'][0]['id']
            channel_name = channel_info['title']
            subscriber_count = response['items'][0]['statistics'].get('subscriberCount', '0')
            view_count = response['items'][0]['statistics'].get('viewCount', '0')
            
            return {
                "name": channel_name,
                "id": channel_id,
                "subscribers": subscriber_count,
                "views": view_count
            }
        else:
            return None
    except HttpError as e:
        print(f"Error accessing channel: {e}")
        return None

def main():
    """Test all tokens and collect channel names into a JSON file."""
    print("\n========================================================")
    print("       YouTube Token Verification & Channel List        ")
    print("========================================================\n")
    
    # Get all token files
    token_files = glob.glob(os.path.join(TOKEN_DIR, "*.pickle"))
    
    if not token_files:
        print(f"No token files found in {TOKEN_DIR}. Please run generate_tokens.py first.")
        return
    
    print(f"Found {len(token_files)} token files to test.\n")
    
    # Dictionary to store channel data
    channels = []
    success_count = 0
    failed_count = 0
    
    # Process each token file
    for i, token_file in enumerate(token_files, 1):
        token_name = os.path.basename(token_file)
        print(f"[{i}/{len(token_files)}] Testing token: {token_name}")
        
        try:
            # Load credentials
            credentials = load_credentials(token_file)
            
            # Build YouTube service
            youtube = build('youtube', 'v3', credentials=credentials)
            
            # Get channel info
            channel_data = get_channel_info(youtube)
            
            if channel_data:
                channels.append(channel_data)
                print(f"  [SUCCESS] Channel '{channel_data['name']}' ({channel_data['id']})")
                print(f"    Subscribers: {channel_data['subscribers']}, Views: {channel_data['views']}")
                success_count += 1
            else:
                print(f"  [FAILED] Could not get channel info")
                failed_count += 1
                
        except Exception as e:
            print(f"  [ERROR] with token {token_name}: {str(e)}")
            failed_count += 1
        
        print("")  # Empty line for readability
    
    # Save channel data to JSON file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(channels, f, indent=2)
    
    # Summary
    print("\n========================================================")
    print(f"SUMMARY: Tested {len(token_files)} tokens")
    print(f"  [SUCCESS]: {success_count} channels accessible")
    print(f"  [FAILED]: {failed_count} channels not accessible")
    print(f"\nChannel data saved to: {OUTPUT_FILE}")
    print("========================================================")

if __name__ == "__main__":
    main()
