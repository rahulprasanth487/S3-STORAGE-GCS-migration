import os
import json
import requests
from typing import List, Dict, Union, Optional
import mimetypes

def list_bucket_contents(bucket_name: str, token: str, prefix: str = None) -> List[Dict]:
    """
    List all contents of the bucket with optional prefix.
    
    Args:
        bucket_name: Name of the GCS bucket
        token: Bearer token for authentication
        prefix: Optional prefix to filter objects (simulate folder)
        
    Returns:
        List of objects in the bucket
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    base_url = f"https://storage.googleapis.com/storage/v1/b/{bucket_name}"
    
    params = {}
    if prefix:
        params["prefix"] = prefix
    
    response = requests.get(
        f"{base_url}/o", 
        headers=headers,
        params=params
    )
    
    if response.status_code == 200:
        result = response.json()
        return result.get("items", [])
    else:
        raise Exception(f"Failed to list bucket contents: {response.text}")

def list_folder_contents(bucket_name: str, token: str, folder_path: str) -> List[Dict]:
    """
    List contents of a specific folder in the bucket.
    
    Args:
        bucket_name: Name of the GCS bucket
        token: Bearer token for authentication
        folder_path: Path to the folder (must end with a /)
        
    Returns:
        List of objects in the specified folder
    """
    # Ensure folder path ends with /
    if not folder_path.endswith('/'):
        folder_path += '/'
        
    return list_bucket_contents(bucket_name, token, prefix=folder_path)

def create_folder(bucket_name: str, token: str, folder_path: str) -> Dict:
    """
    Create a new folder in the bucket.
    GCS doesn't have actual folders, so this creates a zero-byte object ending with /
    
    Args:
        bucket_name: Name of the GCS bucket
        token: Bearer token for authentication
        folder_path: Path of the folder to create (must end with a /)
        
    Returns:
        Response from the GCS API
    """
    # Ensure folder path ends with /
    if not folder_path.endswith('/'):
        folder_path += '/'
        
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
        "Content-Length": "0"
    }
    
    upload_url = f"https://storage.googleapis.com/upload/storage/v1/b/{bucket_name}"
    
    response = requests.post(
        f"{upload_url}/o",
        headers=headers,
        params={
            "uploadType": "media",
            "name": folder_path
        },
        data=""
    )
    
    if response.status_code in [200, 201]:
        return response.json()
    else:
        raise Exception(f"Failed to create folder: {response.text}")

def upload_file(bucket_name: str, token: str, local_file_path: str, gcs_file_path: str) -> Dict:
    """
    Upload a file to the bucket.
    
    Args:
        bucket_name: Name of the GCS bucket
        token: Bearer token for authentication
        local_file_path: Path to the local file
        gcs_file_path: Destination path in GCS
        
    Returns:
        Response from the GCS API
    """
    if not os.path.isfile(local_file_path):
        raise FileNotFoundError(f"File not found: {local_file_path}")
    
    # Detect content type
    content_type, _ = mimetypes.guess_type(local_file_path)
    if content_type is None:
        content_type = "application/octet-stream"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": content_type
    }
    
    upload_url = f"https://storage.googleapis.com/upload/storage/v1/b/{bucket_name}"
    
    with open(local_file_path, 'rb') as file:
        file_data = file.read()
        
    response = requests.post(
        f"{upload_url}/o",
        headers=headers,
        params={
            "uploadType": "media",
            "name": gcs_file_path
        },
        data=file_data
    )
    
    if response.status_code in [200, 201]:
        return response.json()
    else:
        raise Exception(f"Failed to upload file: {response.text}")

def upload_directory(bucket_name: str, token: str, local_dir_path: str, gcs_base_path: str = "") -> List[Dict]:
    """
    Upload an entire directory to the bucket.
    
    Args:
        bucket_name: Name of the GCS bucket
        token: Bearer token for authentication
        local_dir_path: Path to the local directory
        gcs_base_path: Base path in GCS to upload to
        
    Returns:
        List of responses from the GCS API
    """
    if not os.path.isdir(local_dir_path):
        raise NotADirectoryError(f"Directory not found: {local_dir_path}")
    
    # Ensure gcs_base_path ends with / if not empty
    if gcs_base_path and not gcs_base_path.endswith('/'):
        gcs_base_path += '/'
        
    responses = []
    
    # Create the base directory if it doesn't exist and isn't empty
    if gcs_base_path:
        try:
            responses.append(create_folder(bucket_name, token, gcs_base_path))
        except Exception as e:
            # Ignore if folder already exists
            pass
    
    # Walk through the directory
    for root, dirs, files in os.walk(local_dir_path):
        # Calculate the relative path from the local directory
        rel_path = os.path.relpath(root, local_dir_path)
        if rel_path == '.':
            rel_path = ""
            
        # Create all subdirectories
        for dir_name in dirs:
            if rel_path:
                gcs_folder_path = f"{gcs_base_path}{rel_path}/{dir_name}/"
            else:
                gcs_folder_path = f"{gcs_base_path}{dir_name}/"
                
            try:
                responses.append(create_folder(bucket_name, token, gcs_folder_path))
            except Exception as e:
                # Ignore if folder already exists
                pass
        
        # Upload all files
        for file_name in files:
            local_file_path = os.path.join(root, file_name)
            
            if rel_path:
                gcs_file_path = f"{gcs_base_path}{rel_path}/{file_name}"
            else:
                gcs_file_path = f"{gcs_base_path}{file_name}"
                
            try:
                responses.append(upload_file(bucket_name, token, local_file_path, gcs_file_path))
            except Exception as e:
                print(f"Failed to upload {local_file_path}: {str(e)}")
    
    return responses

def upload_files_and_directories(bucket_name: str, token: str, path_mapping: List[Dict[str, str]]) -> List[Dict]:
    """
    Upload multiple files and directories based on a mapping.
    
    Args:
        bucket_name: Name of the GCS bucket
        token: Bearer token for authentication
        path_mapping: List of dictionaries with 'local_path' and 'gcs_path' keys
        
    Returns:
        List of responses from the GCS API
    """
    responses = []
    
    for mapping in path_mapping:
        local_path = mapping.get('local_path')
        gcs_path = mapping.get('gcs_path')
        
        if not local_path or not gcs_path:
            continue
            
        if os.path.isfile(local_path):
            responses.append(upload_file(bucket_name, token, local_path, gcs_path))
        elif os.path.isdir(local_path):
            responses.extend(upload_directory(bucket_name, token, local_path, gcs_path))
        else:
            print(f"Path not found: {local_path}")
            
    return responses

def delete_file(bucket_name: str, token: str, file_path: str) -> bool:
    """
    Delete a file from the bucket.
    
    Args:
        bucket_name: Name of the GCS bucket
        token: Bearer token for authentication
        file_path: Path to the file in GCS
        
    Returns:
        True if successful, False otherwise
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    base_url = f"https://storage.googleapis.com/storage/v1/b/{bucket_name}"
    
    response = requests.delete(
        f"{base_url}/o/{requests.utils.quote(file_path, safe='')}",
        headers=headers
    )
    
    return response.status_code in [200, 204]

def delete_folder(bucket_name: str, token: str, folder_path: str) -> Dict[str, int]:
    """
    Delete a folder and all its contents from the bucket.
    
    Args:
        bucket_name: Name of the GCS bucket
        token: Bearer token for authentication
        folder_path: Path to the folder in GCS
        
    Returns:
        Dictionary with counts of successful and failed deletions
    """
    # Ensure folder path ends with /
    if not folder_path.endswith('/'):
        folder_path += '/'
        
    # List all objects in the folder
    objects = list_bucket_contents(bucket_name, token, prefix=folder_path)
    
    results = {"successful": 0, "failed": 0}
    
    # Delete each object
    for obj in objects:
        name = obj.get('name')
        if delete_file(bucket_name, token, name):
            results["successful"] += 1
        else:
            results["failed"] += 1
            
    return results


# Example usage
if __name__ == "__main__":
    import os
    from google.oauth2 import service_account
    
    # Example of how to get a token from service account
    def get_bearer_token_from_service_account(service_account_file: str) -> str:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        credentials.refresh(requests.Request())
        return credentials.token
    
    # This is just an example - you should get your token using appropriate method
    # token = get_bearer_token_from_service_account('path/to/service-account.json')
    token = "YOUR_BEARER_TOKEN"
    bucket_name = "your-bucket-name"
    
    # 1. List bucket contents
    contents = list_bucket_contents(bucket_name, token)
    print(f"Bucket contents: {len(contents)} items")
    
    # 2. List folder contents
    folder_contents = list_folder_contents(bucket_name, token, "my-folder/")
    print(f"Folder contents: {len(folder_contents)} items")
    
    # 3. Create folder
    new_folder = create_folder(bucket_name, token, "test/new-folder/")
    print(f"Created folder: {new_folder.get('name')}")
    
    # 4. Upload file
    upload_result = upload_file(bucket_name, token, "local-file.txt", "test/new-folder/file.txt")
    print(f"Uploaded file: {upload_result.get('name')}")
    
    # 5. Upload directory
    dir_results = upload_directory(bucket_name, token, "local-directory", "test/new-directory")
    print(f"Uploaded directory: {len(dir_results)} items")
    
    # 6. Upload multiple files/directories
    path_mapping = [
        {"local_path": "file1.txt", "gcs_path": "uploads/file1.txt"},
        {"local_path": "images/", "gcs_path": "uploads/images"}
    ]
    multi_results = upload_files_and_directories(bucket_name, token, path_mapping)
    print(f"Multi upload results: {len(multi_results)} items")
    
    # 7. Delete file
    delete_result = delete_file(bucket_name, token, "test/new-folder/file.txt")
    print(f"File deleted: {delete_result}")
    
    # 8. Delete folder
    folder_delete = delete_folder(bucket_name, token, "test/new-folder/")
    print(f"Folder deletion: {folder_delete}")
