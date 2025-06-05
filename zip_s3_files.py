import boto3
import zipfile
import os
import shutil
from datetime import datetime

BUCKET_NAME = "devdolphins-assignment-bucket"
PREFIX = "pattern_output/"
ZIP_NAME = f"assignment_output_{datetime.now():%Y%m%d_%H%M%S}.zip"
LOCAL_DIR = "temp_s3_download"

def download_files(s3):
    os.makedirs(LOCAL_DIR, exist_ok=True)
    files = []
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)

    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('/') or os.path.basename(key).startswith('_'):
            continue  # skip folders and system files

        local_path = os.path.join(LOCAL_DIR, os.path.relpath(key, PREFIX))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(BUCKET_NAME, key, local_path)
        files.append(local_path)

    return files

def create_zip(files):
    with zipfile.ZipFile(ZIP_NAME, 'w') as zipf:
        for file in files:
            arcname = os.path.relpath(file, LOCAL_DIR)
            zipf.write(file, arcname)

def cleanup():
    shutil.rmtree(LOCAL_DIR)
    os.remove(ZIP_NAME)

def main():
    s3 = boto3.client('s3')
    try:
        files = download_files(s3)
        if not files:
            print("No files found.")
            return

        create_zip(files)
        s3.upload_file(ZIP_NAME, BUCKET_NAME, ZIP_NAME)

        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET_NAME, 'Key': ZIP_NAME},
            ExpiresIn=7 * 24 * 3600
        )

        cleanup()
        print("Download URL:", url)

    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()