import hashlib
from http import HTTPStatus
import logging
import os
import requests

logging.getLogger().setLevel(logging.INFO)

API_KEY = 'YOURKEYHERE'
API_URL = 'https://onechart-prod.appspot.com/partner/upload_url'
UPLOAD_CHUNK_SIZE = 1024*1024


# Could be more memory efficient, but sufficient for this example.
def md5(filepath: str) -> str:
    h = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            h.update(chunk)
    return h.hexdigest()


# See https://cloud.google.com/storage/docs/xml-api/resumable-upload.
def upload(filepath: str, mime_type: str, patient_id: str,
           referrer_npi: str):
    f_size = os.stat(filepath).st_size
    f_md5 = md5(filepath)  # Optional, but recommended.

    # Step 0. Get the signed upload URL.
    logging.info('Getting signed upload URL from OneChart.')
    headers = {
        'Authorization': 'Key %s' % API_KEY,
        'Content-Type': mime_type,
        'Content-MD5': f_md5,
    }
    params = {
        'patientID': patient_id,
        'referrerNPI': referrer_npi,
    }
    resp = requests.get(API_URL, headers=headers, params=params)
    if resp.status_code != HTTPStatus.OK:
        raise Exception(
            'Failed getting signed upload URL. HTTP status: %s' % resp.reason)
    signed_upload_url = resp.json()['url']

    # Step 1. Initiate resumable upload.
    logging.info('Initializing resumable upload.')
    headers = {
        'Content-Length': '0',
        'Content-Type': mime_type,
        'x-goog-resumable': 'start',
    }
    resp = requests.post(signed_upload_url, headers=headers)
    if resp.status_code != HTTPStatus.CREATED:
        raise Exception(
            'Upload failed to initiate. HTTP status: %s' % resp.reason)

    # Step 2. Get the Upload Session URI.
    upload_session_uri = resp.headers['Location']

    # Step 3. Upload the file.
    logging.info('Uploading.')
    f = open(filepath, 'rb')
    headers = {
        'Content-Length': str(f_size),
        'Content-MD5': f_md5,
    }
    resp = requests.put(upload_session_uri, data=f, headers=headers)
    while resp.status_code != HTTPStatus.OK:
        if resp.status_code not in [HTTPStatus.INTERNAL_SERVER_ERROR,
                                    HTTPStatus.SERVICE_UNAVAILABLE]:
            raise Exception(
                'Upload failed. HTTP status: %s' % resp.reason)

        # Step 4. Upload interrupted, query for upload status.
        logging.info('Upload interrupted. Fetching upload status.')
        headers = {
            'Content-Length': '0',
            'Content-Range': 'bytes */%s' % f_size,
        }
        resp = requests.put(upload_session_uri, headers=headers)

        # Step 5. Process upload status query response.
        if resp.status_code != HTTPStatus.PERMANENT_REDIRECT:
            raise Exception(
                'Upload failed to get status. HTTP Status: %s' % resp.reason)
        # Range example: "bytes=0-12345"
        start = int(resp.headers['Range'].split('-')[1]) + 1

        # Step 6. Resume upload.
        logging.info('Upload resuming.')
        headers = {
            'Content-Length': f_size - start,
            'Content-MD5': f_md5,
            'Content-Range': 'bytes %s-%s/%s' % (start, f_size - 1, f_size),
        }
        f.seek(start)
        resp = requests.put(upload_session_uri, data=f, headers=headers)

    logging.info('Upload complete.')


if __name__ == '__main__':
    upload('stuff.zip', 'application/zip', 'foo', '1790857241')
