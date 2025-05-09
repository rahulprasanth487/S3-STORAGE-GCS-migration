"""
Google Cloud Storage Utility Functions

This module provides utility functions for common Google Cloud Storage operations
using bearer token authentication.
"""

from google.cloud import storage
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import os
import glob
from typing import List, Optional, Union


def get_client_with_token(token: str) -> storage.Client:
    """
    Create a Google Cloud Storage client using bearer token authentication.
    
    Args:
        token: The bearer token for authentication
        
    Returns:
        A Google Cloud Storage client
    """
    credentials = Credentials(token=token)
    return storage.Client(credentials=credentials)


def list_bucket_contents(token: str, bucket_name: str, prefix: str = None) -> List[str]:
    """
    List all objects in a bucket, optionally filtered by a prefix.
    
    Args:
        token: The bearer token for authentication
        bucket_name: Name of the bucket
        prefix: Optional prefix to filter objects (e.g., folder path)
        
    Returns:
        List of object names in the bucket
    """
    client = get_client_with_token(token)
    bucket = client.bucket(bucket_name)
    blobs = client.list_blobs(bucket, prefix=prefix)
    
    return [blob.name for blob in blobs]


def list_folder_contents(token: str, bucket_name: str, folder_path: str) -> List[str]:
    """
    List contents of a specific folder in the bucket.
    
    Args:
        token: The bearer token for authentication
        bucket_name: Name of the bucket
        folder_path: Path to the folder (e.g., 'my-folder/' or 'my-folder/subfolder/')
        
    Returns:
        List of object names in the folder
    """
    # Ensure folder path ends with a slash
    if not folder_path.endswith('/'):
        folder_path += '/'
    
    return list_bucket_contents(token, bucket_name, folder_path)


def create_folder(token: str, bucket_name: str, folder_path: str) -> bool:
    """
    Create a folder in the bucket. In GCS, folders are virtual and created by 
    creating an empty object with a name ending in '/'.
    
    Args:
        token: The bearer token for authentication
        bucket_name: Name of the bucket
        folder_path: Path to the folder to create (e.g., 'my-folder/' or 'my-folder/subfolder/')
        
    Returns:
        True if successful
    """
    client = get_client_with_token(token)
    bucket = client.bucket(bucket_name)
    
    # Ensure folder path ends with a slash
    if not folder_path.endswith('/'):
        folder_path += '/'
    
    # Create an empty object with the folder path as its name
    blob = bucket.blob(folder_path)
    blob.upload_from_string('')
    
    return True


def upload_file(token: str, bucket_name: str, source_file_path: str, 
                destination_blob_name: Optional[str] = None) -> str:
    """
    Upload a file to the bucket.
    
    Args:
        token: The bearer token for authentication
        bucket_name: Name of the bucket
        source_file_path: Path to the local file to upload
        destination_blob_name: Optional name for the file in the bucket
                              (if not provided, uses the filename)
        
    Returns:
        The name of the uploaded blob
    """
    client = get_client_with_token(token)
    bucket = client.bucket(bucket_name)
    
    if destination_blob_name is None:
        destination_blob_name = os.path.basename(source_file_path)
    
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    
    return destination_blob_name


def upload_directory(token: str, bucket_name: str, source_dir_path: str, 
                    destination_prefix: str = '') -> List[str]:
    """
    Upload an entire directory to the bucket.
    
    Args:
        token: The bearer token for authentication
        bucket_name: Name of the bucket
        source_dir_path: Path to the local directory to upload
        destination_prefix: Optional prefix to add to the uploaded files in the bucket
        
    Returns:
        List of uploaded blob names
    """
    if not os.path.isdir(source_dir_path):
        raise ValueError(f"Source directory does not exist: {source_dir_path}")
    
    # Ensure destination prefix ends with a slash if it's not empty
    if destination_prefix and not destination_prefix.endswith('/'):
        destination_prefix += '/'
    
    uploaded_files = []
    
    # First, find all files in the directory and its subdirectories
    for root, _, files in os.walk(source_dir_path):
        for filename in files:
            local_path = os.path.join(root, filename)
            
            # Determine the relative path from the source directory
            rel_path = os.path.relpath(local_path, source_dir_path)
            
            # Compute the destination blob name
            dest_blob_name = os.path.join(destination_prefix, rel_path).replace('\\', '/')
            
            # Upload the file
            uploaded_file = upload_file(token, bucket_name, local_path, dest_blob_name)
            uploaded_files.append(uploaded_file)
    
    return uploaded_files


def upload_files_and_directories(token: str, bucket_name: str, 
                                paths: List[str], destination_prefix: str = '') -> List[str]:
    """
    Upload multiple files or directories to the bucket.
    
    Args:
        token: The bearer token for authentication
        bucket_name: Name of the bucket
        paths: List of local file or directory paths to upload
        destination_prefix: Optional prefix to add to the uploaded files in the bucket
        
    Returns:
        List of uploaded blob names
    """
    if destination_prefix and not destination_prefix.endswith('/'):
        destination_prefix += '/'
    
    uploaded_files = []
    
    for path in paths:
        if os.path.isfile(path):
            # It's a file, upload it directly
            dest_blob_name = os.path.join(destination_prefix, os.path.basename(path)).replace('\\', '/')
            uploaded_file = upload_file(token, bucket_name, path, dest_blob_name)
            uploaded_files.append(uploaded_file)
        elif os.path.isdir(path):
            # It's a directory, upload its contents
            dir_name = os.path.basename(path)
            dir_dest_prefix = os.path.join(destination_prefix, dir_name).replace('\\', '/')
            uploaded_dir_files = upload_directory(token, bucket_name, path, dir_dest_prefix)
            uploaded_files.extend(uploaded_dir_files)
        else:
            raise ValueError(f"Path does not exist: {path}")
    
    return uploaded_files


def delete_file(token: str, bucket_name: str, blob_name: str) -> bool:
    """
    Delete a file from the bucket.
    
    Args:
        token: The bearer token for authentication
        bucket_name: Name of the bucket
        blob_name: Name of the blob to delete
        
    Returns:
        True if successful
    """
    client = get_client_with_token(token)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    blob.delete()
    return True


def delete_folder(token: str, bucket_name: str, folder_path: str) -> List[str]:
    """
    Delete a folder and all its contents from the bucket.
    
    Args:
        token: The bearer token for authentication
        bucket_name: Name of the bucket
        folder_path: Path to the folder to delete
        
    Returns:
        List of deleted blob names
    """
    client = get_client_with_token(token)
    bucket = client.bucket(bucket_name)
    
    # Ensure folder path ends with a slash
    if not folder_path.endswith('/'):
        folder_path += '/'
    
    # List all blobs in the folder
    blobs = client.list_blobs(bucket, prefix=folder_path)
    deleted_blobs = []
    
    # Delete each blob
    for blob in blobs:
        blob.delete()
        deleted_blobs.append(blob.name)
    
    return deleted_blobs


# Example usage (commented out)
"""
# Authentication token
bearer_token = "your_bearer_token_here"

# Bucket name
bucket_name = "your-bucket-name"

# List all contents in the bucket
all_items = list_bucket_contents(bearer_token, bucket_name)
print(f"All items in bucket: {all_items}")

# List contents of a specific folder
folder_items = list_folder_contents(bearer_token, bucket_name, "my-folder")
print(f"Items in my-folder: {folder_items}")

# Create a new folder
create_folder(bearer_token, bucket_name, "new-folder")
print("Created new folder: new-folder/")

# Upload a single file
uploaded_file = upload_file(bearer_token, bucket_name, "local-file.txt", "remote-file.txt")
print(f"Uploaded file: {uploaded_file}")

# Upload an entire directory
uploaded_dir_files = upload_directory(bearer_token, bucket_name, "local-directory", "remote-directory")
print(f"Uploaded directory files: {uploaded_dir_files}")

# Upload mixed files and directories
paths_to_upload = ["file1.txt", "file2.jpg", "directory1", "directory2"]
uploaded_items = upload_files_and_directories(bearer_token, bucket_name, paths_to_upload, "uploads")
print(f"Uploaded items: {uploaded_items}")

# Delete a file
delete_file(bearer_token, bucket_name, "remote-file.txt")
print("Deleted file: remote-file.txt")

# Delete a folder
deleted_items = delete_folder(bearer_token, bucket_name, "folder-to-delete")
print(f"Deleted folder items: {deleted_items}")
"""
