import os
import mimetypes
from datetime import datetime, timezone
from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from werkzeug.utils import secure_filename


load_dotenv(override=True)

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
KB_ID = os.getenv("KB_ID")
MODEL_ID = os.getenv("MODEL_ID", "amazon.nova-lite-v1:0")
INFERENCE_PROFILE_ARN = os.getenv("INFERENCE_PROFILE_ARN")
MODEL_ARN = os.getenv("MODEL_ARN")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_UPLOAD_PREFIX = os.getenv("S3_UPLOAD_PREFIX", "uploads/")

if not KB_ID:
	raise ValueError("KB_ID is missing. Set it in .env")

if KB_ID.upper().startswith("YOUR_"):
	raise ValueError("KB_ID still has placeholder value. Set real Knowledge Base ID in .env")

if not S3_BUCKET_NAME:
	raise ValueError("S3_BUCKET_NAME is missing. Set it in .env")


def _build_foundation_model_arn(model_id: str) -> str:
	# Foundation model ARNs do not include an account ID.
	return f"arn:aws:bedrock:{AWS_REGION}::foundation-model/{model_id}"


def _model_target_type(model_arn: str) -> str:
	if "foundation-model/" in model_arn:
		return "foundation-model"
	if "inference-profile/" in model_arn or "application-inference-profile/" in model_arn:
		return "inference-profile"
	return "unknown"


if INFERENCE_PROFILE_ARN:
	ACTIVE_MODEL_ARN = INFERENCE_PROFILE_ARN
elif MODEL_ARN:
	ACTIVE_MODEL_ARN = MODEL_ARN
else:
	ACTIVE_MODEL_ARN = _build_foundation_model_arn(MODEL_ID)

target_type = _model_target_type(ACTIVE_MODEL_ARN)
if target_type == "unknown":
	raise ValueError(
		"Model target ARN must include foundation-model/, inference-profile/, or application-inference-profile/"
	)

if target_type == "foundation-model":
	ACTIVE_MODEL_ID = ACTIVE_MODEL_ARN.split("foundation-model/")[-1]
else:
	# For inference profiles, retain identifier-like suffix for diagnostics.
	ACTIVE_MODEL_ID = ACTIVE_MODEL_ARN.split("/")[-1]

app = Flask(__name__)

# Allow React dev server and any configured frontend origins.
CORS(
	app,
	resources={r"/api/*": {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]}},
)

bedrock_agent_runtime = boto3.client("bedrock-agent-runtime", region_name=AWS_REGION)
bedrock_agent = boto3.client("bedrock-agent", region_name=AWS_REGION)
bedrock = boto3.client("bedrock", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


@app.get("/")
def home():
	return jsonify(
		{
			"message": "Backend is running",
			"endpoints": {
				"health": "/api/health",
				"query": "/api/query",
			},
		}
	)


@app.get("/api/health")
def health():
	return jsonify({"status": "ok", "region": AWS_REGION, "bucket": S3_BUCKET_NAME})


@app.post("/api/upload")
def upload_files():
	if "files" not in request.files:
		return jsonify({"error": "No files field provided"}), 400

	files = request.files.getlist("files")
	if not files:
		return jsonify({"error": "No files uploaded"}), 400

	uploaded_files = []
	for file_storage in files:
		if not file_storage or not file_storage.filename:
			continue

		filename = secure_filename(file_storage.filename)
		if not filename:
			continue

		content_type = file_storage.mimetype or mimetypes.guess_type(filename)[0] or "application/octet-stream"
		timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
		object_key = f"{S3_UPLOAD_PREFIX.rstrip('/')}/{timestamp}_{filename}".lstrip("/")

		try:
			s3.upload_fileobj(
				file_storage,
				S3_BUCKET_NAME,
				object_key,
				ExtraArgs={
					"ContentType": content_type,
					"Metadata": {
						"source": "securevault",
						"original-filename": filename,
					},
				},
			)
			uploaded_files.append(
				{
					"name": filename,
					"type": "pdf" if filename.lower().endswith(".pdf") else "image",
					"sizeBytes": file_storage.content_length,
					"bucket": S3_BUCKET_NAME,
					"key": object_key,
					"uploadedAt": timestamp,
				}
			)
		except ClientError as exc:
			error_info = exc.response.get("Error", {})
			return jsonify(
				{
					"error": error_info.get("Message", str(exc)),
					"errorCode": error_info.get("Code", "ClientError"),
					"bucket": S3_BUCKET_NAME,
				}
			), 400

	if not uploaded_files:
		return jsonify({"error": "No valid files were uploaded"}), 400

	return jsonify(
		{
			"message": "Files uploaded to S3 successfully",
			"bucket": S3_BUCKET_NAME,
			"uploadedFiles": uploaded_files,
		}
	)


@app.get("/api/config-check")
def config_check():
	model_id = ACTIVE_MODEL_ID
	result = {
		"region": AWS_REGION,
		"knowledgeBaseId": KB_ID,
		"modelArn": ACTIVE_MODEL_ARN,
		"modelId": model_id,
		"checks": {},
	}

	# 1) Check whether KB exists and is accessible.
	try:
		kb = bedrock_agent.get_knowledge_base(knowledgeBaseId=KB_ID)
		kb_info = kb.get("knowledgeBase", {})
		result["checks"]["knowledgeBase"] = {
			"ok": True,
			"status": kb_info.get("status"),
			"name": kb_info.get("name"),
		}
	except ClientError as exc:
		error_info = exc.response.get("Error", {})
		result["checks"]["knowledgeBase"] = {
			"ok": False,
			"code": error_info.get("Code", "ClientError"),
			"message": error_info.get("Message", str(exc)),
		}

	# 2) Check whether the selected generation target is accessible.
	try:
		if _model_target_type(ACTIVE_MODEL_ARN) == "foundation-model":
			fm = bedrock.get_foundation_model(modelIdentifier=model_id)
			model_details = fm.get("modelDetails", {})
			result["checks"]["model"] = {
				"ok": True,
				"targetType": "foundation-model",
				"provider": model_details.get("providerName"),
				"responseStreamingSupported": model_details.get("responseStreamingSupported"),
			}
		else:
			profile = bedrock.get_inference_profile(inferenceProfileIdentifier=ACTIVE_MODEL_ARN)
			profile_info = profile.get("inferenceProfile", {})
			result["checks"]["model"] = {
				"ok": True,
				"targetType": "inference-profile",
				"inferenceProfileArn": profile_info.get("inferenceProfileArn"),
				"status": profile_info.get("status"),
			}
	except ClientError as exc:
		error_info = exc.response.get("Error", {})
		result["checks"]["model"] = {
			"ok": False,
			"targetType": _model_target_type(ACTIVE_MODEL_ARN),
			"code": error_info.get("Code", "ClientError"),
			"message": error_info.get("Message", str(exc)),
		}

	# 3) Retrieval-only test isolates KB/vector issues from generation-model issues.
	try:
		retr = bedrock_agent_runtime.retrieve(
			knowledgeBaseId=KB_ID,
			retrievalQuery={"text": "health check"},
			retrievalConfiguration={
				"vectorSearchConfiguration": {
					"numberOfResults": 5,
					"overrideSearchType": "SEMANTIC",
				}
			},
		)
		results = retr.get("retrievalResults", [])
		result["checks"]["retrieve"] = {
			"ok": True,
			"resultsFound": len(results),
		}
	except ClientError as exc:
		error_info = exc.response.get("Error", {})
		result["checks"]["retrieve"] = {
			"ok": False,
			"code": error_info.get("Code", "ClientError"),
			"message": error_info.get("Message", str(exc)),
		}

	# 4) Data source and ingestion status help detect unsynced/failed KBs.
	try:
		ds_response = bedrock_agent.list_data_sources(knowledgeBaseId=KB_ID, maxResults=20)
		data_sources = []
		for ds in ds_response.get("dataSourceSummaries", []):
			ds_id = ds.get("dataSourceId")
			ds_item = {
				"dataSourceId": ds_id,
				"name": ds.get("name"),
				"status": ds.get("status"),
			}

			if ds_id:
				try:
					jobs = bedrock_agent.list_ingestion_jobs(
						knowledgeBaseId=KB_ID,
						dataSourceId=ds_id,
						maxResults=1,
					)
					latest_job = (jobs.get("ingestionJobSummaries") or [None])[0]
					if latest_job:
						ds_item["latestIngestionJob"] = {
							"status": latest_job.get("status"),
							"updatedAt": str(latest_job.get("updatedAt")),
							"failureReasons": latest_job.get("failureReasons", []),
						}
				except ClientError as exc:
					error_info = exc.response.get("Error", {})
					ds_item["latestIngestionJob"] = {
						"status": "UNKNOWN",
						"error": error_info.get("Message", str(exc)),
					}

			data_sources.append(ds_item)

		result["checks"]["dataSources"] = {"ok": True, "items": data_sources}
	except ClientError as exc:
		error_info = exc.response.get("Error", {})
		result["checks"]["dataSources"] = {
			"ok": False,
			"code": error_info.get("Code", "ClientError"),
			"message": error_info.get("Message", str(exc)),
		}

	all_ok = all(check.get("ok") for check in result["checks"].values())
	status_code = 200 if all_ok else 400
	return jsonify(result), status_code


@app.get("/test")
def test_page():
	return render_template_string(
		"""
		<!doctype html>
		<html>
		<head>
			<meta charset=\"utf-8\" />
			<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
			<title>KB Test</title>
			<style>
				body { font-family: Segoe UI, sans-serif; max-width: 720px; margin: 32px auto; padding: 0 16px; }
				textarea { width: 100%; min-height: 120px; margin: 8px 0 12px; }
				button { padding: 10px 16px; cursor: pointer; }
				pre { background: #f4f6f8; padding: 12px; white-space: pre-wrap; border-radius: 8px; }
			</style>
		</head>
		<body>
			<h2>Knowledge Base Test</h2>
			<p>Enter a question and click Ask.</p>
			<textarea id=\"query\" placeholder=\"Type your question...\"></textarea>
			<br />
			<button id=\"askBtn\">Ask</button>
			<h3>Answer</h3>
			<pre id=\"answer\">No response yet.</pre>

			<script>
				document.getElementById("askBtn").addEventListener("click", async () => {
					const query = document.getElementById("query").value.trim();
					const answerEl = document.getElementById("answer");
					if (!query) {
						answerEl.textContent = "Please enter a query.";
						return;
					}

					answerEl.textContent = "Loading...";
					try {
						const res = await fetch("/api/query", {
							method: "POST",
							headers: { "Content-Type": "application/json" },
							body: JSON.stringify({ query })
						});
						const data = await res.json();
						if (!res.ok) {
							answerEl.textContent = data.error || "Request failed";
							return;
						}
						answerEl.textContent = data.answer || "No answer returned";
					} catch (err) {
						answerEl.textContent = err.message;
					}
				});
			</script>
		</body>
		</html>
		"""
	)


def _build_answer_input(user_query: str) -> str:
	return (
		"Answer the question using only the knowledge base context. "
		"Return a concise direct answer. Do not output tool calls, action logs, "
		"or diagnostic text.\n\n"
		f"Question: {user_query}"
	)


def _clean_model_answer(answer: str) -> str:
	if not answer:
		return answer

	trimmed = answer.strip()
	if trimmed.startswith("Action:") or "GlobalDataSource.search" in trimmed:
		return "I could not generate a direct answer from the knowledge base."
	return answer


@app.post("/api/query")
def query_knowledge_base():
	data = request.get_json(silent=True) or {}
	user_query = (data.get("query") or "").strip()

	if not user_query:
		return jsonify({"error": "No query provided"}), 400

	try:
		response = bedrock_agent_runtime.retrieve_and_generate(
			input={"text": _build_answer_input(user_query)},
			retrieveAndGenerateConfiguration={
				"type": "KNOWLEDGE_BASE",
				"knowledgeBaseConfiguration": {
					"knowledgeBaseId": KB_ID,
					"modelArn": ACTIVE_MODEL_ARN,
					"retrievalConfiguration": {
						"vectorSearchConfiguration": {
							"numberOfResults": 5,
							"overrideSearchType": "SEMANTIC",
						}
					},
					"generationConfiguration": {
						"inferenceConfig": {
							"textInferenceConfig": {
								"maxTokens": 400,
								"temperature": 0.2,
							}
						}
					},
				},
			},
		)

		answer = _clean_model_answer(response.get("output", {}).get("text", ""))

		citations = []
		for citation in response.get("citations", []):
			refs = citation.get("retrievedReferences", [])
			for ref in refs:
				citations.append(
					{
						"text": ref.get("content", {}).get("text", ""),
						"location": ref.get("location", {}),
						"metadata": ref.get("metadata", {}),
					}
				)

		return jsonify({"answer": answer, "citations": citations})

	except ClientError as exc:
		error_info = exc.response.get("Error", {})
		error_code = error_info.get("Code", "ClientError")
		error_message = error_info.get("Message", str(exc))
		request_id = exc.response.get("ResponseMetadata", {}).get("RequestId")
		payment_issue = "INVALID_PAYMENT_INSTRUMENT" in error_message or "Marketplace subscription" in error_message
		throughput_issue = "on-demand throughput" in error_message or "inference profile" in error_message

		hints = [
			"KB_ID must belong to the same AWS account and region as AWS_REGION.",
			"Set INFERENCE_PROFILE_ARN (preferred), or MODEL_ARN, to a valid Bedrock inference profile ARN in this region.",
			"If using MODEL_ID directly, ensure the model supports on-demand throughput in your region.",
			"Your IAM user/role needs bedrock:RetrieveAndGenerate and knowledge base permissions.",
		]

		if payment_issue:
			hints.insert(0, "This model is blocked by AWS billing/Marketplace. Add a valid payment instrument or switch to Amazon Nova.")
		if throughput_issue:
			hints.insert(0, "This model requires an inference profile in your region. Configure INFERENCE_PROFILE_ARN and retry.")

		return jsonify(
			{
				"error": error_message,
				"errorCode": error_code,
				"requestId": request_id,
				"paymentIssueDetected": payment_issue,
				"throughputIssueDetected": throughput_issue,
				"model": {
					"modelId": ACTIVE_MODEL_ID,
					"modelArn": ACTIVE_MODEL_ARN,
					"targetType": _model_target_type(ACTIVE_MODEL_ARN),
				},
				"debug": {
					"region": AWS_REGION,
					"knowledgeBaseId": KB_ID,
					"modelArn": ACTIVE_MODEL_ARN,
					"hints": hints,
				},
			}
		), 400

	except Exception as exc:
		return jsonify({"error": str(exc)}), 500


@app.get("/api/query")
def query_knowledge_base_help():
	return jsonify(
		{
			"message": "Use POST /api/query with JSON body: {\"query\": \"your question\"}",
			"example": {
				"method": "POST",
				"url": "/api/query",
				"body": {"query": "What is this knowledge base about?"},
			},
		}
	)


@app.post("/query")
def query_knowledge_base_legacy():
	# Backward-compatible route for older frontend calls.
	return query_knowledge_base()


@app.get("/query")
def query_knowledge_base_legacy_help():
	return query_knowledge_base_help()


if __name__ == "__main__":
	app.run(host="0.0.0.0", port=5000, debug=True)
