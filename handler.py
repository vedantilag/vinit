import boto3
import os
import urllib.parse
import time

s3 = boto3.client("s3")
textract = boto3.client("textract")

PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]


def lambda_handler(event, context):
    print("LAMBDA STARTED")

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    print("Processing:", key)

    if not key.lower().endswith((".png", ".jpg", ".jpeg", ".pdf")):
        print("Unsupported file type")
        return {"statusCode": 200}

    local_path = "/tmp/input_file"
    s3.download_file(bucket, key, local_path)

    # Extract text
    text = extract_with_textract(bucket, key, local_path)

    # ✅ STEP 4 — CLEAN TEXT ADDED HERE
    text = clean_text(text)

    if not text.strip():
        raise Exception("No text extracted from document")

    output_key = f"extracted/{key}.txt"

    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=output_key,
        Body=text.encode("utf-8"),
        ContentType="text/plain",
        Metadata={
            "source-file": key,
            "file-type": key.split(".")[-1]
        }
    )

    print("Extraction completed successfully")

    return {"statusCode": 200}


# -----------------------------
# TEXTRACT FUNCTION
# -----------------------------
def extract_with_textract(bucket, key, local_path):

    if key.lower().endswith(".pdf"):
        # Start async job for PDF
        response = textract.start_document_text_detection(
            DocumentLocation={
                "S3Object": {
                    "Bucket": bucket,
                    "Name": key
                }
            }
        )

        job_id = response["JobId"]

        print("Textract Job ID:", job_id)

        # Wait for completion
        while True:
            result = textract.get_document_text_detection(JobId=job_id)
            status = result["JobStatus"]

            print("Job Status:", status)

            if status in ["SUCCEEDED", "FAILED"]:
                break

            time.sleep(3)  # Prevent tight loop

        if status == "FAILED":
            raise Exception("Textract PDF job failed")

        lines = [
            block["Text"]
            for block in result["Blocks"]
            if block["BlockType"] == "LINE"
        ]

        return "\n".join(lines)

    else:
        # For images
        with open(local_path, "rb") as f:
            file_bytes = f.read()

        response = textract.detect_document_text(
            Document={"Bytes": file_bytes}
        )

        lines = [
            block["Text"]
            for block in response["Blocks"]
            if block["BlockType"] == "LINE"
        ]

        return "\n".join(lines)


# -----------------------------
# STEP 4 — CLEANING FUNCTION
# -----------------------------
def clean_text(text):
    # Remove extra spaces
    text = " ".join(text.split())

    # Normalize new lines
    text = text.replace("\r", "\n")

    # Remove problematic characters safely
    text = text.encode("utf-8", "ignore").decode("utf-8")

    return text
