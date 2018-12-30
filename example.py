from http import HTTPStatus
import logging
import os
import requests
from typing import Optional

logging.getLogger().setLevel(logging.INFO)

# Test partner (OID=000000000000000000000000)
API_KEY = '4f1cbf8e7f7cdd651efd9a7b0a8f50d77c37acb7'
API_SECRET = 'c3237759042595d306f33dec551187dfdb2c0d46'
API_URL = 'https://api.onecharthealth.com/partners/upload_url'
UPLOAD_CHUNK_SIZE = 1024*1024


# See https://cloud.google.com/storage/docs/xml-api/resumable-upload.
def upload(filepath: str, mime_type: str, patient_id: str, file_type: str,
           file_description: str, referring_npi: Optional[int]=None,
           rendering_npi: Optional[int]=None,
           accession_number: Optional[str]=None):
    f_size = os.stat(filepath).st_size

    # Step 0. Get the signed upload URL from OneChart.
    logging.info('Getting signed upload URL from OneChart.')
    headers = {'Authorization': f'Key {API_KEY}:{API_SECRET}'}
    body = {
        'patientID': patient_id,
        'description': file_description,
        'type': file_type,
        'Content-Type': mime_type,
    }
    if referring_npi:
        body['referringNPI'] = referring_npi
    if rendering_npi:
        body['renderingNPI'] = rendering_npi
    if accession_number:
        body['accessionNumber'] = accession_number
    resp = requests.post(API_URL, headers=headers, json=body)
    if resp.status_code != HTTPStatus.OK:
        s = resp.status_code
        b = resp.content
        raise Exception(
            f'Failed getting signed upload URL. HTTP status: {s}, body: {b}')
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
            f'Upload failed to initiate. HTTP status: {resp.reason}')

    # Step 2. Get the Upload Session URI.
    upload_session_uri = resp.headers['Location']

    # Step 3. Upload the file.
    logging.info('Uploading.')
    f = open(filepath, 'rb')
    headers = {
        'Content-Length': str(f_size),
        # 'Content-MD5': f_md5,
    }
    resp = requests.put(upload_session_uri, data=f, headers=headers)
    while resp.status_code != HTTPStatus.OK:
        if resp.status_code not in [HTTPStatus.INTERNAL_SERVER_ERROR,
                                    HTTPStatus.SERVICE_UNAVAILABLE]:
            raise Exception(
                f'Upload failed. HTTP status: {resp.reason}')

        # Step 4. Upload interrupted, query for upload status.
        logging.info('Upload interrupted. Fetching upload status.')
        headers = {
            'Content-Length': '0',
            'Content-Range': f'bytes */{f_size}',
        }
        resp = requests.put(upload_session_uri, headers=headers)

        # Step 5. Process upload status query response.
        if resp.status_code != HTTPStatus.PERMANENT_REDIRECT:
            raise Exception(
                f'Upload failed to get status. HTTP Status:  {resp.reason}')
        # Range example: "bytes=0-12345"
        start = int(resp.headers['Range'].split('-')[1]) + 1

        # Step 6. Resume upload.
        logging.info('Upload resuming.')
        headers = {
            'Content-Length': f_size - start,
            # 'Content-MD5': f_md5,
            'Content-Range': f'bytes {start}-{f_size-1}/{f_size}',
        }
        f.seek(start)
        resp = requests.put(upload_session_uri, data=f, headers=headers)

    logging.info('Upload complete.')


if __name__ == '__main__':
    filepath = input('Filepath of file to upload: ')
    mimetype = input('Mimetype of file to upload: ')
    file_type = input('Type of file (e.g. MRI): ')
    file_desc = input('Description of file: ')
    patient_id = input('Patient ID: ')
    referring_npi = input('Referring NPI (optional): ')
    referring_npi = int(referring_npi) if referring_npi else None
    rendering_npi = input('Rendering NPI (optional): ')
    rendering_npi = int(rendering_npi) if rendering_npi else None
    accession_number = input('Accession Number (optional): ') or None
    upload(
        filepath, mimetype, patient_id, file_type, file_desc,
        referring_npi=referring_npi, rendering_npi=rendering_npi,
        accession_number=accession_number)
