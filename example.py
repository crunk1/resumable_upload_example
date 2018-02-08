import hashlib
from http import HTTPStatus
import os
import requests

UPLOAD_CHUNK_SIZE = 1024*1024


def get_size(filepath: str) -> int:
    st = os.stat(filepath)
    return st.st_size


# Could be more memory efficient, but sufficient for this example.
def md5(filepath: str) -> str:
    h = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            h.update(chunk)
    return h.hexdigest()


# See https://cloud.google.com/storage/docs/xml-api/resumable-upload.
def upload_zip(filepath: str):
    content_length = get_size(filepath)
    content_md5 = md5(filepath)  # Optional, but recommended.
    content_type = 'application/zip'
    signed_upload_url = requests.get('https://stuff/upload_url')

    # Step 1. Initiate resumable upload.
    headers = {
        'Content-Length': 0,
        'Content-Type': content_type,
        'x-goog-resumable': 'start',
    }
    resp = requests.post(signed_upload_url, headers=headers)
    if resp.status_code != HTTPStatus.CREATED:
        raise Exception(
            'Upload failed to initiate. HTTP status: %s' % resp.reason)

    # Step 2. Get the Upload Session URI.
    upload_session_uri = resp.headers['Location']

    # Step 3. Upload the file.
    f = open(filepath, 'rb')
    headers = {
        'Content-Length': content_length,
        'Content-MD5': content_md5,
    }
    resp = requests.put(upload_session_uri, data=f, headers=headers)
    while resp.status_code != HTTPStatus.OK:
        if resp.status_code in [HTTPStatus.INTERNAL_SERVER_ERROR,
                                HTTPStatus.SERVICE_UNAVAILABLE]:
            pass

