#!/usr/bin/env python3

import os
import json
import sys
import time
import datetime
import argparse
import io
import requests  # Added for Telegram API calls
import shutil  # Added for file cleanup operations
import random  # Added for random video selection
import pickle  # Added for loading credentials from pickle files
import re

# Force UTF-8 encoding for all file operations and console output
sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
import google.oauth2.credentials
import googleapiclient.discovery
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import httplib2
import http.client
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Constants from Google Drive script
DRIVE_SHEETS_SCOPES = ['https://www.googleapis.com/auth/drive', 
                       'https://www.googleapis.com/auth/spreadsheets']
TARGET_FOLDER_NAME = "All video"  # Updated folder name for video uploads
DIRECT_FOLDER_MODE = True  # Set to True to process videos directly from the main folder instead of subfolders
SPREADSHEET_ID = '1sqrNVHGo181vGsl1hF5GPUXiQZBUmC_O0KLNBFdMa9g'  # Updated spreadsheet ID
SHEET_NAME = 'Sheet1'
SPREADSHEET_ID_FILE = 'spreadsheet_id.txt'
MAX_VIDEOS_PER_CHANNEL = 5  # Safety limit for testing
TEMP_DIR = 'temp_download'

# YouTube upload constants
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError, http.client.NotConnected,
                        http.client.IncompleteRead, http.client.ImproperConnectionState,
                        http.client.CannotSendRequest, http.client.CannotSendHeader,
                        http.client.ResponseNotReady, http.client.BadStatusLine)
MAX_RETRIES = 10
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
VALID_PRIVACY_STATUSES = ("public", "private", "unlisted")
CHANNEL_TOKENS_DIR = 'acces_channel_token'  
CHANNEL_MAPPINGS_FILE = 'channel_names.json'  
DEFAULT_YOUTUBE_CHANNEL = "AniSnap"  # Default channel to use if none specified

# Telegram Bot configuration
TELEGRAM_BOT_TOKEN = "7603358162:AAGITSily1F2J2j_dvSHno1ST8UmRdiWSDw"
TELEGRAM_CHAT_ID = "-1002882576277"  # Chat ID from the message link
TELEGRAM_THREAD_ID = 263  # Thread ID from the message link
TELEGRAM_NOTIFICATIONS_ENABLED = True  # Set to False to disable notifications

# New spreadsheet columns for tracking uploads
UPLOAD_TRACKING_COLUMNS = [
    'Upload Status',    # Yes/No/Failed
    'Upload Date',      # Timestamp 
    'YouTube URL',      # Full video URL
    'YouTube Channel',  # Channel name used
    'YouTube Video ID', # Video ID
    'Error Message'     # Only populated if failed
]

def get_google_drive_credentials():
    """Get credentials for Google Drive and Sheets API."""
    # Google Drive link for credentials.json
    CREDENTIALS_DRIVE_LINK = "https://drive.google.com/file/d/1YtEywz1VR6YMdKmMjwTo2EtDcNjwUVHS/view?usp=drive_link"
    credentials_file = os.path.join('gd', 'credentials.json')
    
    # Try to download from Google Drive first
    try:
        print("Downloading Google Drive credentials from Google Drive...")
        
        # Convert view URL to direct download URL
        file_id = CREDENTIALS_DRIVE_LINK.split('/d/')[1].split('/view')[0]
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        
        # Create temp directory if it doesn't exist
        os.makedirs(TEMP_DIR, exist_ok=True)
        temp_credentials_file = os.path.join(TEMP_DIR, 'temp_credentials.json')
        
        # Download the file
        response = requests.get(download_url)
        if response.status_code == 200:
            with open(temp_credentials_file, 'wb') as f:
                f.write(response.content)
            
            # Load the credentials from the downloaded file
            credentials = service_account.Credentials.from_service_account_file(
                temp_credentials_file, scopes=DRIVE_SHEETS_SCOPES)
            
            # Clean up the temporary file
            os.remove(temp_credentials_file)
            return credentials
        else:
            print(f"Failed to download credentials from Google Drive: {response.status_code}")
            # Fall back to local file
    except Exception as e:
        print(f"Error downloading credentials from Google Drive: {e}")
        # Fall back to local file
    
    # Fall back to local file if download fails
    if os.path.exists(credentials_file):
        print("Using local credentials file...")
        credentials = service_account.Credentials.from_service_account_file(
            credentials_file, scopes=DRIVE_SHEETS_SCOPES)
        return credentials
    else:
        raise FileNotFoundError(f"Google Drive credentials file not found: {credentials_file}")

def get_youtube_credentials(channel_id=None, channel_name=None):
    """Get YouTube credentials based on channel ID or name."""
    print(f"Using channel: {channel_name if channel_name else channel_id}")
    
    # Channel name to Drive file ID mapping
    CHANNEL_DRIVE_LINKS = {
        "ChhotiKahaniBadiBaat": "https://drive.google.com/file/d/12qZIkIH4mL95gx_JxF-d8upbcIKodITS/view?usp=drive_link",
        "KahaniChitra": "https://drive.google.com/file/d/1U8d8EA3RlPQ8Jx3Nt-72_TjYNVX-hUvR/view?usp=drive_link"
    }
    
    if not channel_name and not channel_id:
        print("No channel specified. Please provide channel_id or channel_name.")
        return None
    
    if channel_name:
        if channel_name not in CHANNEL_DRIVE_LINKS:
            print(f"Error: Channel name '{channel_name}' not recognized.")
            print(f"Available channels: {', '.join(CHANNEL_DRIVE_LINKS.keys())}")
            return None
            
        token_url = CHANNEL_DRIVE_LINKS[channel_name]
        
        # Extract file ID from Google Drive URL
        file_id_match = re.search(r'\/d\/([^\/]+)', token_url)
        if not file_id_match:
            print(f"Invalid Drive URL format for channel {channel_name}")
            return None
            
        token_file_id = file_id_match.group(1)
        
        # Download token pickle file from Google Drive
        token_dir = f"tokens_{channel_name}"
        os.makedirs(token_dir, exist_ok=True)
        token_path = os.path.join(token_dir, "token.pickle")
        
        print("Downloading token from Google Drive...")
        credentials = get_google_drive_credentials()
        drive_service = build('drive', 'v3', credentials=credentials)
        
        try:
            request = drive_service.files().get_media(fileId=token_file_id)
            
            with open(token_path, 'wb') as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
            
            # Load credentials from the downloaded pickle file
            with open(token_path, 'rb') as token:
                creds = pickle.load(token)
                
            # Check if token is expired and refresh if needed
            if creds.expired:
                print(f"Refreshing expired token for {channel_name}...")
                creds.refresh(Request())
                # Save the refreshed token
                with open(token_path, 'wb') as token:
                    pickle.dump(creds, token)
            
            return creds
            
        except Exception as e:
            print(f"Error downloading/loading token for {channel_name}: {e}")
            return None
    else:
        print("Channel ID-based authentication not implemented yet.")
        return None

def list_available_youtube_channels():
    """List all channels that have saved YouTube tokens on Google Drive."""
    # Channel token Google Drive links
    CHANNEL_DRIVE_LINKS = {
        "ChhotiKahaniBadiBaat": "https://drive.google.com/file/d/12qZIkIH4mL95gx_JxF-d8upbcIKodITS/view?usp=drive_link",
        "KahaniChitra": "https://drive.google.com/file/d/1U8d8EA3RlPQ8Jx3Nt-72_TjYNVX-hUvR/view?usp=drive_link"
    }
    
    print("\nAvailable YouTube Channels:")
    print("=" * 60)
    
    for i, (channel_name, drive_link) in enumerate(CHANNEL_DRIVE_LINKS.items(), 1):
        print(f"{i}. {channel_name}")
        print(f"   Status: Available on Google Drive")
        print(f"   Drive Link: {drive_link}")
        print()
    
    return CHANNEL_DRIVE_LINKS

def select_channel_interactive():
    """Allow user to select a channel interactively."""
    channels = list_available_youtube_channels()
    
    if not channels:
        return None, None
    
    # Convert to list for easier indexing
    channel_items = list(channels.items())
    
    try:
        choice = int(input("\nEnter the number of the channel to use: "))
        if 1 <= choice <= len(channel_items):
            selected_channel_name, _ = channel_items[choice-1]
            return selected_channel_name, selected_channel_name
    except ValueError:
        pass
    
    print("Invalid selection.")
    return None, None

def download_files_from_folder(folder_id, folder_name):
    """Download all files from a Google Drive folder."""
    credentials = get_google_drive_credentials()
    drive_service = build('drive', 'v3', credentials=credentials)
    
    # Create temporary folder for downloads
    temp_folder = os.path.join(TEMP_DIR, folder_name)
    os.makedirs(temp_folder, exist_ok=True)
    
    try:
        # List all files in the folder
        query = f"'{folder_id}' in parents and trashed = false"
        results = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, mimeType, size)',
            pageSize=50  # Limit results to 50 files
        ).execute()
        
        items = results.get('files', [])
        
        if not items:
            print(f"No files found in folder: {folder_name}")
            return []
            
        print(f"Found {len(items)} files in folder {folder_name}")
        
        downloaded_files = []
        
        # Download each file
        for item in items:
            file_id = item['id']
            file_name = item['name']
            mime_type = item['mimeType']
            
            # Skip Google Docs formats (need export)
            if 'google-apps' in mime_type:
                print(f"Skipping Google Docs format file: {file_name}")
                continue
                
            # Download file
            output_path = os.path.join(temp_folder, file_name)
            
            # Video files can be large - show progress
            if mime_type.startswith('video/') or file_name.lower().endswith(('.mp4', '.mov', '.avi')):
                download_with_progress(drive_service, file_id, output_path, file_name)
            else:
                # Smaller files - simple download
                request = drive_service.files().get_media(fileId=file_id)
                with open(output_path, 'wb') as f:
                    downloader = MediaIoBaseDownload(f, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                
                print(f"Downloading {file_name}: 100%")
            
            downloaded_files.append(output_path)
        
        return downloaded_files
        
    except Exception as e:
        print(f"Error downloading files: {e}")
        return []

def download_with_progress(drive_service, file_id, output_path, file_name):
    """Download a file with progress indication."""
    request = drive_service.files().get_media(fileId=file_id)
    
    with open(output_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            progress = int(status.progress() * 100)
            print(f"Downloading {file_name}: {progress}%", end='\r')
        print(f"Downloading {file_name}: 100%")

def read_text_file(file_path, default=""):
    """Read text content from a file, with a default if file doesn't exist."""
    if not os.path.exists(file_path):
        return default
        
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            return f.read().strip()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return default

def upload_video_to_youtube(video_path, title, description, tags, category="22", 
                          privacy_status="public", credentials=None, channel_title=None):
    """Upload a video to YouTube using the provided credentials."""
    print(f"Uploading video to {channel_title}...")
    
    if not os.path.exists(video_path):
        print(f"Error: Video file not found: {video_path}")
        return None
        
    youtube = build('youtube', 'v3', credentials=credentials)
    
    # Prepare the request body for the API call
    body = {
        'snippet': {
            'title': title,
            'description': description,
            'tags': tags,
            'categoryId': category
        },
        'status': {
            'privacyStatus': privacy_status,
            'selfDeclaredMadeForKids': False
        }
    }
    
    # Call the API to insert (upload) the video
    try:
        # Create a MediaFileUpload object
        media = MediaFileUpload(video_path, 
                              chunksize=1024*1024,
                              resumable=True)
                              
        # Create the insert request
        insert_request = youtube.videos().insert(
            part=','.join(body.keys()),
            body=body,
            media_body=media
        )
        
        # Execute the upload with resumable behavior
        video_id = resumable_upload(insert_request, channel_title)
        return video_id
        
    except HttpError as e:
        print(f"HTTP error uploading video: {e.resp.status} {e.content}")
        return None
    except Exception as e:
        print(f"Error uploading video: {str(e)}")
        return None

def set_thumbnail(youtube, video_id, thumbnail_path):
    """Set a custom thumbnail for a YouTube video."""
    if not os.path.exists(thumbnail_path):
        print(f"Thumbnail file not found: {thumbnail_path}")
        return False
    
    try:
        # Upload the thumbnail
        media = MediaFileUpload(thumbnail_path, mimetype='image/jpeg')
        
        # Set as video thumbnail
        youtube.thumbnails().set(
            videoId=video_id,
            media_body=media
        ).execute()
        
        print(f"Custom thumbnail set for video ID: {video_id}")
        return True
    
    except Exception as e:
        print(f"Error setting thumbnail: {e}")
        return False

def resumable_upload(insert_request, channel_title=None):
    """Execute the resumable upload with retry logic."""
    response = None
    error = None
    retry = 0
    
    # Identify which channel is being used for the upload
    channel_msg = f" to {channel_title}" if channel_title else ""
    
    while response is None:
        try:
            print(f"Uploading video{channel_msg}...")
            status, response = insert_request.next_chunk()
            if response is not None:
                if 'id' in response:
                    video_id = response['id']
                    print(f"Video successfully uploaded! Video ID: {video_id}")
                    print(f"Video URL: https://www.youtube.com/watch?v={video_id}")
                    return video_id
                else:
                    print(f"Upload failed with unexpected response: {response}")
                    return None
        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = f"A retriable HTTP error {e.resp.status} occurred:\n{e.content}"
            else:
                print(f"HTTP error {e.resp.status} occurred:\n{e.content}")
                raise
        except RETRIABLE_EXCEPTIONS as e:
            error = f"A retriable error occurred: {e}"
            
        if error is not None:
            print(error)
            retry += 1
            if retry > MAX_RETRIES:
                print("No longer attempting to retry.")
                return None
                
            max_sleep = 2 ** retry
            sleep_seconds = random.random() * max_sleep
            print(f"Sleeping {sleep_seconds:.1f} seconds and then retrying...")
            time.sleep(sleep_seconds)
            error = None
    
    return None

def send_telegram_notification(video_id, title, channel_title, folder_name):
    """Send a notification to Telegram when a video is uploaded."""
    if not TELEGRAM_NOTIFICATIONS_ENABLED:
        return
        
    try:
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        
        # Create custom notification message with emojis
        message = f"""🔥 Hey Boss! I'm your social media manager, reporting in! 📢

Just a quick update — your latest YouTube upload is LIVE and thriving on {channel_title}! 🚀
We're keeping your content machine running smooth and steady. 🧠💼

📂 Source Folder: {folder_name}
📝 Title: {title}
🔗 Watch Now: {video_url}

Your digital farm is growing fast — and this new crop is packed with viral flavor! 🍎🍊
Next up: I'll be tracking performance, engagement, and prepping the next upload. You just focus on the content — I've got the rest. 💪

Stay tuned, Boss!
#YouTubeUpdate #SocialMediaManager #ExplainShortGuy #DigitalHarvest #ContentEmpire"""
        
        # Send message to Telegram
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "message_thread_id": TELEGRAM_THREAD_ID,
            "text": message
        }
        
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code == 200:
            print(f"Telegram notification sent successfully")
        else:
            print(f"Telegram notification failed: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"Error sending Telegram notification: {e}")

def cleanup_downloaded_files(folder_path):
    """Delete downloaded files after successful upload to save disk space."""
    if not os.path.exists(folder_path):
        return
        
    try:
        print(f"Cleaning up downloaded files in {folder_path}")
        shutil.rmtree(folder_path)
        print(f"Successfully deleted temporary files")
    except Exception as e:
        print(f"Error cleaning up files: {e}")

def process_folder_for_upload(folder_data, row_index, channel_id=None, channel_name=None):
    """Process a single folder for upload to YouTube."""
    folder_id = folder_data.get('Folder ID')
    folder_name = folder_data.get('Subfolder Name')
    
    # Get the row index directly from folder_data if available and valid
    if (row_index is None or row_index <= 0) and 'row_index' in folder_data and folder_data['row_index'] > 0:
        row_index = folder_data['row_index']
        print(f"Using row index {row_index} from folder data")
    
    # As a last resort, find the row index by searching the spreadsheet
    if row_index is None or row_index <= 0:
        row_index = find_row_by_folder_id(folder_id)
        if row_index:
            print(f"Found row index {row_index} by searching spreadsheet")
        else:
            print("Warning: Could not determine row index for updating spreadsheet")
    
    if not folder_id or not folder_name:
        print(f"Invalid folder data: {folder_data}")
        return False
    
    print(f"\nProcessing folder: {folder_name} (ID: {folder_id})")
    
    # Check if already uploaded
    if folder_data.get('Upload Status') == 'Yes' and folder_data.get('YouTube URL'):
        print(f"Folder {folder_name} has already been uploaded to YouTube: {folder_data.get('YouTube URL')}")
        print("Skipping upload.")
        return True
    
    # Create temporary directory for downloads
    temp_folder = os.path.join(TEMP_DIR, folder_name)
    os.makedirs(temp_folder, exist_ok=True)
    
    # Download files from the folder
    files = download_files_from_folder(folder_id, folder_name)
    
    if not files:
        error_msg = f"No files found in folder {folder_name}."
        print(error_msg)
        # update_spreadsheet_row(row_index, None, None, "Failed", error_msg)
        return False
    
    # Check for required files
    video_file = None
    thumbnail_file = None
    
    for file_path in files:
        file_name = os.path.basename(file_path).lower()
        
        # Check for video files
        if file_name.endswith(('.mp4', '.mov', '.avi', '.wmv', '.flv', '.mkv')):
            video_file = file_path
            
        # Check for thumbnail files
        elif file_name.endswith(('.jpg', '.jpeg', '.png')):
            thumbnail_file = file_path
    
    if not video_file:
        error_msg = f"No video file found in folder {folder_name}."
        print(error_msg)
        # update_spreadsheet_row(row_index, None, None, "Failed", error_msg)
        return False
    
    # Read metadata files if they exist
    title = read_text_file(os.path.join(temp_folder, 'title.txt'), folder_name)
    description = read_text_file(os.path.join(temp_folder, 'description.txt'), f"Video from {folder_name}")
    tags_string = read_text_file(os.path.join(temp_folder, 'tags.txt'), "")
    tags = [tag.strip() for tag in tags_string.split(',')] if tags_string else []
    
    # Get the YouTube channel to upload to
    if not channel_id and not channel_name:
        channel_name = DEFAULT_YOUTUBE_CHANNEL
    
    # Get credentials based on the stored token folders from the memory
    token_folder = None
    # Channel to token folder mapping
    if channel_name == "Explainshortguy":
        token_folder = "tokens_explainshortguy"
    elif channel_name == "FlickFlash":
        token_folder = "tokens_flickflash"
    
    # Get credentials for this channel
    credentials = None
    if token_folder and os.path.exists(token_folder):
        token_path = os.path.join(token_folder, "token.pickle")
        if os.path.exists(token_path):
            try:
                with open(token_path, 'rb') as token_file:
                    credentials = pickle.load(token_file)
                print(f"Successfully loaded credentials from {token_path}")
            except Exception as e:
                print(f"Error loading credentials from {token_path}: {e}")
    
    # Fall back to downloading from Drive if token not found locally
    if not credentials:
        credentials = get_youtube_credentials(channel_id, channel_name)
    
    if not credentials:
        error_msg = f"Failed to get credentials for channel: {channel_name or channel_id}"
        print(error_msg)
        # update_spreadsheet_row(row_index, None, channel_name, "Failed", error_msg)
        return False
    
    try:
        # Upload the video
        video_id = upload_video_to_youtube(
            video_file,
            title,
            description,
            tags,
            credentials=credentials,
            channel_title=channel_name
        )
        
        if not video_id:
            error_msg = "Upload failed with unknown error."
            print(error_msg)
            # update_spreadsheet_row(row_index, None, channel_name, "Failed", error_msg)
            return False
            
        print(f"Video successfully uploaded! Video ID: {video_id}")
        print(f"Video URL: https://www.youtube.com/watch?v={video_id}")
        
        # Set thumbnail if available
        if thumbnail_file:
            try:
                youtube = build('youtube', 'v3', credentials=credentials)
                set_thumbnail(youtube, video_id, thumbnail_file)
            except Exception as e:
                print(f"Error setting thumbnail: {e}")
        
        # update_spreadsheet_row(row_index, video_id, channel_name, "Yes")
        
        # Send notification
        send_telegram_notification(video_id, title, channel_name, folder_name)
        
        # Clean up downloaded files
        cleanup_downloaded_files(temp_folder)
        
        return True
        
    except Exception as e:
        error_msg = f"Upload failed: {str(e)}"
        print(error_msg)
        # update_spreadsheet_row(row_index, None, channel_name, "Failed", error_msg)
        return False

def process_unuploaded_videos(channel_id=None, channel_name=None, limit=None, random_selection=True):
    """Process videos directly from the main folder using random selection by default."""
    # Get the folder ID for the main folder
    target_folder_id = find_target_folder_id()
    if not target_folder_id:
        print(f"Error: Could not find folder '{TARGET_FOLDER_NAME}' in Google Drive.")
        return False
        
    # Process videos directly from the folder
    return process_direct_folder_videos(
        target_folder_id,
        channel_id=channel_id,
        channel_name=channel_name, 
        limit=limit,
        random_selection=random_selection
    )

def process_direct_folder_videos(folder_id, channel_id=None, channel_name=None, limit=None, random_selection=True):
    """Process videos directly from the main folder."""
    credentials = get_google_drive_credentials()
    drive_service = build('drive', 'v3', credentials=credentials)
    
    try:
        # Query to find all video files in the folder
        query = f"'{folder_id}' in parents and trashed = false and (mimeType contains 'video/' or name contains '.mp4' or name contains '.mov' or name contains '.avi')"
        
        # Retrieve all videos using pagination - not just the first 100
        all_video_files = []
        page_token = None
        
        print(f"Retrieving all videos from {TARGET_FOLDER_NAME} folder...")
        
        while True:
            # Get one page of results
            results = drive_service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType, createdTime)',
                pageToken=page_token,
                pageSize=100  # Maximum allowed page size
            ).execute()
            
            # Add this page's files to our list
            video_files = results.get('files', [])
            all_video_files.extend(video_files)
            
            # Get next page token, if any
            page_token = results.get('nextPageToken', None)
            
            # If we have no more pages, break the loop
            if not page_token:
                break
        
        # Now all_video_files contains all videos from the folder
        if not all_video_files:
            print(f"No video files found in {TARGET_FOLDER_NAME} folder.")
            return False
            
        print(f"Found {len(all_video_files)} video files in {TARGET_FOLDER_NAME} folder.")
        
        # Apply limit and random selection if specified
        if random_selection and limit:
            # Randomly select videos up to the limit
            if limit > len(all_video_files):
                limit = len(all_video_files)
            
            print(f"Randomly selecting {limit} videos for upload from the entire folder.")
            selected_videos = random.sample(all_video_files, limit)
        else:
            # Take the first N videos based on limit
            selected_videos = all_video_files[:limit] if limit else all_video_files
        
        print(f"Processing {len(selected_videos)} videos{' (limited by --limit)' if limit else ''}.")
        
        success_count = 0
        fail_count = 0
        
        # Create temp directory for downloads
        os.makedirs(TEMP_DIR, exist_ok=True)
        
        # Get the YouTube channel to upload to
        if not channel_id and not channel_name:
            channel_name = DEFAULT_YOUTUBE_CHANNEL
            
        # Get credentials for this channel
        credentials = get_youtube_credentials(channel_id, channel_name)
        
        if not credentials:
            error_msg = f"Failed to get credentials for channel: {channel_name or channel_id}"
            print(error_msg)
            return False
        
        # Process each video
        for idx, video_file in enumerate(selected_videos):
            print(f"\n============================================================")
            print(f"Processing {idx+1}/{len(selected_videos)}: {video_file.get('name', '')}")
            print(f"============================================================\n")
            
            file_id = video_file['id']
            file_name = video_file['name']
            
            # Use filename as title (without extension)
            title = os.path.splitext(file_name)[0]
            
            # Create temporary folder for this video
            temp_folder = os.path.join(TEMP_DIR, f"video_{idx}")
            os.makedirs(temp_folder, exist_ok=True)
            
            # Download the video file
            video_path = os.path.join(temp_folder, file_name)
            try:
                request = drive_service.files().get_media(fileId=file_id)
                with open(video_path, 'wb') as f:
                    downloader = MediaIoBaseDownload(f, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        progress = int(status.progress() * 100)
                        print(f"Downloading {file_name}: {progress}%", end='\r')
                print(f"Downloading {file_name}: 100%")
                
                # Basic description and tags
                description = f"Anime shorts video: {title}"
                tags = ["anime", "shorts", "animation"]
                
                # Upload the video
                try:
                    # Upload the video
                    video_id = upload_video_to_youtube(
                        video_path,
                        title,
                        description,
                        tags,
                        privacy_status="public",  # Set videos to public by default
                        credentials=credentials,
                        channel_title=channel_name
                    )
                    
                    if not video_id:
                        print(f"Upload failed for {file_name}")
                        fail_count += 1
                        continue
                        
                    print(f"Video successfully uploaded! Video ID: {video_id}")
                    print(f"Video URL: https://www.youtube.com/watch?v={video_id}")
                    
                    # Add entry to spreadsheet
                    add_video_to_spreadsheet(video_id, title, channel_name)
                    
                    # Send notification
                    send_telegram_notification(video_id, title, channel_name, TARGET_FOLDER_NAME)
                    success_count += 1
                    
                except Exception as e:
                    print(f"Error uploading video {file_name}: {e}")
                    fail_count += 1
                    
            except Exception as e:
                print(f"Error downloading video {file_name}: {e}")
                fail_count += 1
                
            finally:
                # Clean up downloaded files
                cleanup_downloaded_files(temp_folder)
        
        print(f"\n============================================================")
        print(f"Upload Summary")
        print(f"============================================================")
        print(f"Total processed: {success_count + fail_count}")
        print(f"Successful: {success_count}")
        print(f"Failed: {fail_count}")
        print(f"============================================================\n")
        
        return success_count > 0
        
    except Exception as e:
        print(f"Error processing direct folder videos: {e}")
        return False

def add_video_to_spreadsheet(video_id, video_title, channel_name, upload_status="Success"):
    """Add an entry to the spreadsheet for each uploaded video."""
    credentials = get_google_drive_credentials()
    sheets_service = build('sheets', 'v4', credentials=credentials)
    
    try:
        # Current date/time string
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        video_url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
        
        # Data row to append
        row_data = [
            video_id,                  # Video ID
            video_title,               # Video Title
            channel_name,              # Channel Name
            upload_status,             # Upload Status
            now,                       # Upload Date
            video_url                  # Video URL
        ]
        
        # Append to spreadsheet
        sheets_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!A:F',
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body={'values': [row_data]}
        ).execute()
        
        print(f"Successfully added entry to spreadsheet for video: {video_title}")
        return True
        
    except Exception as e:
        print(f"Error adding video to spreadsheet: {e}")
        return False

def find_target_folder_id():
    """Find the target folder ID for the News folder."""
    credentials = get_google_drive_credentials()
    drive_service = build('drive', 'v3', credentials=credentials)
    
    try:
        # Search for the News folder by name
        query = f"name = '{TARGET_FOLDER_NAME}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        items = results.get('files', [])
        
        if not items:
            print(f"Error: Could not find folder '{TARGET_FOLDER_NAME}' in Google Drive.")
            return None
        
        # Use the first matching folder
        folder_id = items[0]['id']
        print(f"Found {TARGET_FOLDER_NAME} folder with ID: {folder_id}")
        return folder_id
        
    except Exception as e:
        print(f"Error finding target folder: {e}")
        return None

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Upload videos from Google Drive to YouTube")
    
    # Channel selection
    channel_group = parser.add_argument_group("Channel Selection")
    channel_group.add_argument("--list-channels", action="store_true", help="List available channels and exit")
    channel_group.add_argument("--channel-id", help="Channel ID to upload to")
    channel_group.add_argument("--channel-name", help="Channel name to upload to (will try to match)")
    
    # Upload options
    upload_group = parser.add_argument_group("Upload Options")
    upload_group.add_argument("--limit", type=int, help="Limit the number of videos to upload")
    upload_group.add_argument("--folder-name", help="Only upload from a specific folder name")
    upload_group.add_argument("--privacy-status", choices=VALID_PRIVACY_STATUSES, default="public",
                            help="Privacy status for uploaded videos")
    upload_group.add_argument("--random", action="store_true", help="Randomly select videos for upload")
    
    args = parser.parse_args()
    
    # Create temp directory if it doesn't exist
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    # Just list channels if requested
    if args.list_channels:
        list_available_youtube_channels()
        return
    
    # Process unuploaded videos
    process_unuploaded_videos(
        channel_id=args.channel_id,
        channel_name=args.channel_name,
        limit=args.limit,
        random_selection=True  # Always use random selection
    )

if __name__ == "__main__":
    main()
