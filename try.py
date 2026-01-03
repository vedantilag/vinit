import boto3
import os
import io
import re
import logging
from datetime import datetime
from io import BytesIO

# ================= SAFE THIRD-PARTY IMPORTS =================

try:
    from docx import Document
    from docx.table import Table
    from docx.text.paragraph import Paragraph
except ImportError as e:
    raise ImportError("Missing dependency: python-docx") from e

try:
    from PIL import Image
except ImportError as e:
    raise ImportError("Missing dependency: Pillow") from e

try:
    import PyPDF2
except ImportError as e:
    raise ImportError("Missing dependency: PyPDF2") from e

try:
    import fitz  # PyMuPDF
except ImportError as e:
    raise ImportError("Missing dependency: PyMuPDF") from e


# ================= LOGGING =================

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ================= CONFIG =================

OUTPUT_PREFIX = "trail-base-vedant-2025/"
INPUT_PREFIX = "uploads/"
TARGET_BUCKET = "my-document-uploads-vedant-2025"

s3 = boto3.client("s3")


# ================= HELPERS =================

def get_s3_object(event):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    logger.info(f"Incoming file: s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    return key, obj["Body"].read()


def put_s3_object(key, content, content_type="application/octet-stream"):
    s3.put_object(
        Bucket=TARGET_BUCKET,
        Key=key,
        Body=content,
        ContentType=content_type
    )


def preprocess_text(text):
    text = text.lower()
    allowed = r"a-z0-9\|\.\,\!\?\:\;\'\"\(\)\[\]\{\}\-\_\/\\"
    text = re.sub(rf"[^\n{allowed} ]", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = re.sub(r"\n+", "\n", text)
    return text


def resize_image_if_needed(img):
    max_dim = 2048
    max_pixels = 2048 * 2048
    w, h = img.size
    scale = min(max_dim / w, max_dim / h, 1.0)

    while int(w * scale) * int(h * scale) > max_pixels:
        scale *= 0.95

    if scale < 1:
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)

    return img


# ================= PROCESSORS =================

def process_txt(content, base_key):
    text = preprocess_text(content.decode("utf-8", errors="ignore"))
    put_s3_object(f"{base_key}/txt", text.encode(), "text/plain")


def process_image(content, base_key):
    img = Image.open(BytesIO(content))
    img = resize_image_if_needed(img)
    out = BytesIO()
    img.save(out, format="PNG")
    put_s3_object(f"{base_key}/img.png", out.getvalue(), "image/png")


def process_pdf(content, base_key):
    # -------- TEXT --------
    reader = PyPDF2.PdfReader(BytesIO(content))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    text = preprocess_text(text)
    put_s3_object(f"{base_key}/txt", text.encode(), "text/plain")

    # -------- IMAGES --------
    doc = fitz.open(stream=content, filetype="pdf")
    idx = 1

    for page in doc:
        for img in page.get_images(full=True):
            base = doc.extract_image(img[0])
            if len(base["image"]) < 512:
                continue

            im = Image.open(BytesIO(base["image"]))
            im = resize_image_if_needed(im)
            out = BytesIO()
            im.save(out, format="PNG")
            put_s3_object(f"{base_key}/img_{idx}.png", out.getvalue(), "image/png")
            idx += 1


def iter_docx_blocks(doc):
    from docx.oxml.table import CT_Tbl
    from docx.oxml.text.paragraph import CT_P

    for child in doc.element.body.iterchildren():
        if isinstance(child, CT_P):
            yield Paragraph(child, doc)
        elif isinstance(child, CT_Tbl):
            yield Table(child, doc)


def process_docx(content, base_key):
    doc = Document(BytesIO(content))
    lines = []

    for block in iter_docx_blocks(doc):
        if isinstance(block, Paragraph):
            lines.append(block.text)
        elif isinstance(block, Table):
            for row in block.rows:
                lines.append(" | ".join(cell.text for cell in row.cells))
            lines.append("")

    text = preprocess_text("\n".join(lines))
    put_s3_object(f"{base_key}/txt", text.encode(), "text/plain")

    idx = 1
    for rel in doc.part.rels.values():
        if "image" in rel.reltype:
            img = Image.open(BytesIO(rel.target_part.blob))
            img = resize_image_if_needed(img)
            out = BytesIO()
            img.save(out, format="PNG")
            put_s3_object(f"{base_key}/img_{idx}.png", out.getvalue(), "image/png")
            idx += 1


# ================= ROUTER =================

def process_file(key, content):
    ext = os.path.splitext(key)[1].lower()
    name = os.path.basename(key)
    date = datetime.now().strftime("%d-%b-%y")
    base_key = f"{OUTPUT_PREFIX}{date}/{name}"

    logger.info(f"Processing file type: {ext}")

    if ext == ".txt":
        process_txt(content, base_key)
    elif ext == ".pdf":
        process_pdf(content, base_key)
    elif ext == ".docx":
        process_docx(content, base_key)
    elif ext in [".png", ".jpg", ".jpeg"]:
        process_image(content, base_key)
    else:
        logger.warning(f"Unsupported file type: {ext}")


# ================= LAMBDA ENTRY =================

def lambda_handler(event, context):
    try:
        key, content = get_s3_object(event)

        # Skip folders
        if key.endswith("/"):
            return

        # Prevent recursion
        if key.startswith(OUTPUT_PREFIX):
            return

        # Only process uploads
        if not key.startswith(INPUT_PREFIX):
            return

        process_file(key, content)

        logger.info("Processing completed successfully")

    except Exception as e:
        logger.exception("Fatal error during Lambda execution")
        raise
