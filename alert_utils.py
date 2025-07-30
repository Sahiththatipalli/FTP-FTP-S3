import boto3
import os

SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
sns_client = boto3.client("sns")

def send_file_transfer_sns_alert(
    trace_id,
    s3_files,
    ftp_files,
    checksum_results,
    errors=None,
    warnings=None,
    function_name="N/A"
):
    """
    Sends an SNS alert with transfer status, checksums, and any errors/warnings.
    """
    body = f"""AWS Lambda File Transfer Alert
Function: {function_name}
Trace ID: {trace_id}

==== Transfer Status ====
- S3: {'SUCCESS (' + ', '.join(s3_files) + ')' if s3_files else 'NO FILES'}
- FTP: {'SUCCESS (' + ', '.join(ftp_files) + ')' if ftp_files else 'NO FILES'}

==== Checksum Results ====
""" + "\n".join([f"- {c['file']}: {c['status']}" for c in checksum_results])

    if warnings:
        body += "\nWarnings:\n" + "\n".join([str(w) for w in warnings])
    if errors:
        body += "\nErrors:\n" + "\n".join([str(e) for e in errors])

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"File Transfer Alert (Trace ID: {trace_id})",
        Message=body
    )
