import os
import json
import boto3
import paramiko
import tempfile
import shutil
import logging
import ftplib
import time

from logging_utils import (
    log_job_start, log_job_end, log_sftp_connection, log_matched_files,
    log_checksum_ok, log_checksum_fail, log_file_transferred, log_archive,
    log_tmp_usage, log_warning, log_error
)
from checksum_utils import log_checksum
from trace_utils import get_or_create_trace_id
from file_match_utils import match_files
from retry_utils import default_retry
from storage_utils import get_date_subpath
from performance_utils import time_operation
from metrics_utils import publish_file_transfer_metric, publish_error_metric
from alert_utils import send_file_transfer_sns_alert

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("paramiko").setLevel(logging.WARNING)

s3_client = boto3.client('s3')

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = response['SecretString']
    return json.loads(secret)

def get_file_patterns():
    val = os.getenv('FILE_PATTERN')
    if val:
        return [x.strip() for x in val.split(',') if x.strip()]
    return ['*']

@default_retry()
def create_sftp_client(host, port, username, password):
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    return paramiko.SFTPClient.from_transport(transport)

@default_retry()
def download_and_upload_to_s3(sftp_client, remote_dir, bucket, prefix, local_dir, trace_id, job_id, file_patterns, metrics, transfer_status, checksum_status, errors, warnings):
    all_files = sftp_client.listdir(remote_dir)
    files = match_files(all_files, include_patterns=file_patterns)
    unmatched = set(all_files) - set(files)
    date_subpath = get_date_subpath()
    log_matched_files(trace_id, files, unmatched)

    total_bytes = 0
    t0 = time.time()
    for filename in files:
        remote_path = f"{remote_dir}/{filename}"
        local_path = os.path.join(local_dir, filename)

        _, duration = time_operation(sftp_client.get, remote_path, local_path)
        bytes_transferred = os.path.getsize(local_path)
        total_bytes += bytes_transferred

        downloaded_checksum = log_checksum(local_path, trace_id, algo="sha256", note="after SFTP download")
        s3_upload_checksum = log_checksum(local_path, trace_id, algo="sha256", note="before S3 upload")

        if downloaded_checksum == s3_upload_checksum:
            log_checksum_ok(trace_id, filename, downloaded_checksum)
            checksum_status[filename] = f"OK (sha256: {downloaded_checksum})"
        else:
            log_checksum_fail(trace_id, filename, downloaded_checksum, s3_upload_checksum)
            checksum_status[filename] = f"FAIL (downloaded: {downloaded_checksum}, s3: {s3_upload_checksum})"

        s3_key = f"{prefix}/{date_subpath}/{filename}" if prefix else f"{date_subpath}/{filename}"
        _, s3_duration = time_operation(s3_client.upload_file, local_path, bucket, s3_key)
        log_file_transferred(trace_id, filename, "S3", s3_duration)
        log_archive(trace_id, filename, s3_key)

    t1 = time.time()
    download_time = t1 - t0
    mb = total_bytes / 1024 / 1024 if total_bytes else 0.0
    mbps = (mb / download_time) if download_time else 0.0
    metrics["S3 upload speed mb/s"] = f"{mbps:.2f}"
    metrics["S3 total mb"] = f"{mb:.2f}"
    metrics["SFTP download speed mb/s"] = f"{mbps:.2f}"
    metrics["SFTP total mb"] = f"{mb:.2f}"

    transfer_status["s3"] = f"SUCCESS ({', '.join(files)})" if files else "NO FILES"
    try:
        publish_file_transfer_metric(
            namespace='LambdaFileTransfer',
            direction='SFTP_TO_S3',
            file_count=len(files),
            total_bytes=total_bytes,
            duration_sec=round(download_time,2),
            trace_id=trace_id
        )
    except Exception as e:
        log_error(trace_id, "CloudWatch metric error for S3 transfer", exc=e)
        publish_error_metric('LambdaFileTransfer', 'S3MetricError', trace_id)
        errors.append(str(e))

@default_retry()
def upload_files_to_external_ftp(ftp_host, ftp_user, ftp_pass, remote_dir, local_dir, trace_id, job_id, file_patterns, metrics, transfer_status, checksum_status, errors, warnings):
    files = match_files(os.listdir(local_dir), include_patterns=file_patterns)
    unmatched = set(os.listdir(local_dir)) - set(files)
    date_subpath = get_date_subpath()
    full_path = f"{remote_dir.rstrip('/')}/{date_subpath}"
    parts = full_path.strip('/').split('/')

    log_matched_files(trace_id, files, unmatched)

    ftp = ftplib.FTP(ftp_host)
    ftp.login(ftp_user, ftp_pass)
    for part in parts:
        try:
            ftp.mkd(part)
        except Exception:
            pass
        ftp.cwd(part)

    total_bytes = 0
    t0 = time.time()
    for filename in files:
        local_path = os.path.join(local_dir, filename)
        ftp_upload_checksum = log_checksum(local_path, trace_id, algo="sha256", note="before FTP upload")

        with open(local_path, 'rb') as f:
            _, ftp_duration = time_operation(ftp.storbinary, f'STOR {filename}', f)
            bytes_transferred = os.path.getsize(local_path)
            total_bytes += bytes_transferred
            log_file_transferred(trace_id, filename, "FTP", ftp_duration)
        checksum_status[filename] = f"OK (sha256: {ftp_upload_checksum})"

    t1 = time.time()
    ftp.quit()
    upload_time = t1 - t0
    mb = total_bytes / 1024 / 1024 if total_bytes else 0.0
    mbps = (mb / upload_time) if upload_time else 0.0
    metrics["FTP upload speed mb/s"] = f"{mbps:.2f}"
    metrics["FTP total mb"] = f"{mb:.2f}"

    transfer_status["ftp"] = f"SUCCESS ({', '.join(files)})" if files else "NO FILES"
    try:
        publish_file_transfer_metric(
            namespace='LambdaFileTransfer',
            direction='LOCAL_TO_FTP',
            file_count=len(files),
            total_bytes=total_bytes,
            duration_sec=round(upload_time,2),
            trace_id=trace_id
        )
    except Exception as e:
        log_error(trace_id, "CloudWatch metric error for FTP transfer", exc=e)
        publish_error_metric('LambdaFileTransfer', 'FtpMetricError', trace_id)
        errors.append(str(e))

def lambda_handler(event, context):
    trace_id = get_or_create_trace_id(context)
    job_id = trace_id
    file_patterns = get_file_patterns()
    log_job_start(trace_id, job_id, file_patterns)

    src_secret_name = os.getenv('SRC_SECRET_NAME')
    ext_secret_name = os.getenv('EXT_SECRET_NAME')
    s3_bucket = os.getenv('S3_BUCKET', 'jams-ftp-process-bucket')
    s3_prefix = os.getenv('S3_PREFIX', 'ftp-ftp-list')
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

    src_secret = get_secret(src_secret_name)
    src_host = src_secret['Host']
    src_user = src_secret['Username']
    src_pass = src_secret['Password']
    src_dir = os.getenv('SRC_REMOTE_DIR', '.')

    ext_secret = get_secret(ext_secret_name)
    external_ftp_host = ext_secret['host']
    external_ftp_user = ext_secret['Username']
    external_ftp_pass = ext_secret['password']
    external_ftp_dir = os.getenv('EXT_REMOTE_DIR', '/')

    # --- METRICS, STATUS, CHECKSUM dicts ---
    metrics = {}
    transfer_status = {}
    checksum_status = {}
    errors = []
    warnings = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

        src_sftp = create_sftp_client(src_host, 22, src_user, src_pass)
        log_sftp_connection(trace_id, src_host, "OPENED")

        # SFTP -> S3 (and download metric)
        download_and_upload_to_s3(
            src_sftp, src_dir, s3_bucket, s3_prefix, tmp_dir, trace_id, job_id,
            file_patterns, metrics, transfer_status, checksum_status, errors, warnings
        )
        src_sftp.close()
        log_sftp_connection(trace_id, src_host, "CLOSED")

        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

        # Local -> FTP
        upload_files_to_external_ftp(
            external_ftp_host, external_ftp_user, external_ftp_pass, external_ftp_dir,
            tmp_dir, trace_id, job_id, file_patterns, metrics, transfer_status, checksum_status, errors, warnings
        )

        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

    # Send SNS Alert
    send_file_transfer_sns_alert(
        trace_id=trace_id,
        s3_files=[f for f in transfer_status.get("s3", "").split() if f],
        ftp_files=[f for f in transfer_status.get("ftp", "").split() if f],
        checksum_results=[{'file': k, 'status': v} for k, v in checksum_status.items()],
        errors=errors,
        warnings=warnings,
        function_name="lambda_handler"
    )

    log_job_end(trace_id, job_id)
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Files transferred successfully to S3 and FTP destinations.', 'trace_id': trace_id})
    }
