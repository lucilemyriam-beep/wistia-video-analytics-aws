"""Wistia video analytics: resumable Bronze ingestion with FR7 media watermarks."""

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
SECRET_NAME = "wistia-video-analytics/api-token"
API_VERSION = "2026-09"
MEDIA_IDS = ["8hunphufxp", "9k4tbcdfg0"]
PAGE_SIZE = 100
PAGES_PER_INVOCATION = 15
MIN_REMAINING_MS = 25000
CHECKPOINT_KEY = "checkpoints/wistia/bronze_ingestion_state.json"
MEDIA_WATERMARK_KEY = "checkpoints/wistia/media_watermarks.json"

s3 = boto3.client("s3", region_name=REGION)
secrets = boto3.client("secretsmanager", region_name=REGION)


def write_json(bucket, key, value):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def read_json(bucket, key, default=None):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404", "NotFound"):
            return default
        raise


def read_checkpoint(bucket):
    return read_json(bucket, CHECKPOINT_KEY)


def save_checkpoint(bucket, state):
    state["last_updated_at"] = datetime.now(timezone.utc).isoformat()
    write_json(bucket, CHECKPOINT_KEY, state)


def load_media_watermarks(bucket):
    return read_json(bucket, MEDIA_WATERMARK_KEY, default={})


def save_media_watermarks(bucket, watermarks):
    write_json(bucket, MEDIA_WATERMARK_KEY, watermarks)


def api_token():
    response = secrets.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response["SecretString"])["WISTIA_API_TOKEN"]


def fetch_json(url, token, modern=False):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if modern:
        headers["X-Wistia-API-Version"] = API_VERSION

    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 2:
                raise
            time.sleep(2 ** attempt)
        except urllib.error.URLError:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def sanitize(value, token):
    if isinstance(value, list):
        return [sanitize(item, token) for item in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            name = key.lower()
            if name in ("ip", "ip_address", "ipaddress", "client_ip"):
                result[key + "_hash"] = (
                    hmac.new(token.encode(), str(item).encode(), hashlib.sha256).hexdigest()
                    if item is not None and item != "" else None
                )
            elif name in ("email", "email_address", "visitor_email"):
                continue
            else:
                result[key] = sanitize(item, token)
        return result
    return value


def get_page(kind, page, token, media_id=None):
    params = {"page": page, "per_page": PAGE_SIZE}
    if media_id is not None:
        params["media_id"] = media_id
    url = "https://api.wistia.com/modern/stats/" + kind + "?" + urllib.parse.urlencode(params)
    response = fetch_json(url, token, modern=True)
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for key in ("data", "results", "items"):
            if isinstance(response.get(key), list):
                return response[key]
    raise ValueError("Unexpected paginated response structure")


def initial_state():
    now = datetime.now(timezone.utc)
    return {
        "run_id": now.strftime("%Y%m%dT%H%M%S%fZ"),
        "base_path": "bronze/wistia/" + now.strftime("year=%Y/month=%m/day=%d"),
        "phase": "visitors",
        "next_page": 1,
        "visitor_count": 0,
        "visitor_pages": 0,
        "event_counts": {media_id: 0 for media_id in MEDIA_IDS},
        "event_pages": {media_id: 0 for media_id in MEDIA_IDS},
        "event_index": 0,
        "media_saved": False,
    }


def save_media(bucket, state, token):
    """FR7: fetch metadata, save changed media, and advance persistent watermarks."""
    watermarks = load_media_watermarks(bucket)
    new_watermarks = watermarks.copy()
    records = []

    for media_id in MEDIA_IDS:
        metadata_url = f"https://api.wistia.com/v1/medias/{media_id}.json"
        metadata = fetch_json(metadata_url, token)
        created_at = metadata.get("created")
        updated_at = metadata.get("updated")
        latest_timestamp = updated_at or created_at
        previous_timestamp = watermarks.get(media_id)

        if (previous_timestamp is not None and latest_timestamp is not None
                and latest_timestamp <= previous_timestamp):
            continue

        stats_url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"
        stats = fetch_json(stats_url, token)
        records.append({
            "media_id": metadata.get("id"),
            "hashed_id": metadata.get("hashed_id"),
            "title": metadata.get("name"),
            "created_at": created_at,
            "updated_at": updated_at,
            "duration": metadata.get("duration"),
            "engagement_metrics": {
                name: stats.get(name)
                for name in ("load_count", "play_count", "play_rate", "hours_watched", "engagement")
            },
        })
        if latest_timestamp is not None:
            new_watermarks[media_id] = latest_timestamp

    media_key = state["base_path"] + "/media/media_" + state["run_id"] + ".json"
    write_json(bucket, media_key, {
        "source": "wistia", "run_id": state["run_id"], "records": records,
    })
    # Persist only after successfully writing the media file.
    save_media_watermarks(bucket, new_watermarks)
    state["media_saved"] = True
    state["media_records_written"] = len(records)
    save_checkpoint(bucket, state)


def response_for(state, status_code, message, processed=0):
    return {
        "statusCode": status_code,
        "body": json.dumps({
            "message": message,
            "run_id": state["run_id"],
            "phase": state["phase"],
            "next_page": state["next_page"],
            "pages_processed_this_invocation": processed,
            "visitor_count": state["visitor_count"],
            "event_counts": state["event_counts"],
            "media_saved": state.get("media_saved", False),
            "media_records_written": state.get("media_records_written"),
            "checkpoint_key": CHECKPOINT_KEY,
            "media_watermark_key": MEDIA_WATERMARK_KEY,
        }),
    }


def lambda_handler(event, context):
    event = event if isinstance(event, dict) else {}
    bucket = os.environ["S3_BUCKET_NAME"]

    if event.get("action") == "test_s3":
        key = "bronze/test/s3_connection_test.json"
        write_json(bucket, key, {
            "message": "S3 write successful",
            "tested_at": datetime.now(timezone.utc).isoformat(),
        })
        return {"statusCode": 200, "body": json.dumps({"bucket": bucket, "key": key})}

    state = read_checkpoint(bucket)
    if state is not None and state.get("phase") == "complete":
        if event.get("action") == "start_new_run":
            state = initial_state()
            save_checkpoint(bucket, state)
        else:
            return response_for(state, 200, "Historical extraction already complete; no duplicate run started")

    if state is None:
        state = initial_state()
        save_checkpoint(bucket, state)

    token = api_token()
    if not state.get("media_saved", False):
        save_media(bucket, state, token)

    processed = 0
    while (processed < PAGES_PER_INVOCATION
           and context.get_remaining_time_in_millis() > MIN_REMAINING_MS):
        phase = state["phase"]
        if phase == "complete":
            break

        page = state["next_page"]
        media_id = MEDIA_IDS[state["event_index"]] if phase == "events" else None
        batch = get_page(phase, page, token, media_id)

        if batch:
            if phase == "visitors":
                key = (state["base_path"] + "/visitors/run_id=" + state["run_id"]
                       + f"/page_{page:06d}.json")
            else:
                key = (state["base_path"] + "/events/media_id=" + media_id
                       + "/run_id=" + state["run_id"] + f"/page_{page:06d}.json")
            write_json(bucket, key, {
                "source": "wistia", "run_id": state["run_id"],
                "page": page, "media_id": media_id,
                "records": sanitize(batch, token),
            })

        if phase == "visitors":
            state["visitor_count"] += len(batch)
            state["visitor_pages"] += 1
        else:
            state["event_counts"][media_id] += len(batch)
            state["event_pages"][media_id] += 1

        state["next_page"] += 1
        if len(batch) < PAGE_SIZE:
            if phase == "visitors":
                state["phase"] = "events"
                state["next_page"] = 1
            elif state["event_index"] + 1 < len(MEDIA_IDS):
                state["event_index"] += 1
                state["next_page"] = 1
            else:
                state["phase"] = "complete"

        save_checkpoint(bucket, state)
        processed += 1

    if state["phase"] == "complete":
        manifest_key = (state["base_path"] + "/manifests/manifest_"
                        + state["run_id"] + ".json")
        write_json(bucket, manifest_key, {
            "source": "wistia", "run_id": state["run_id"],
            "media_count": len(MEDIA_IDS),
            "media_records_written": state.get("media_records_written"),
            "visitor_count": state["visitor_count"],
            "visitor_pages": state["visitor_pages"],
            "visitor_complete": True,
            "event_counts": state["event_counts"],
            "event_pages": state["event_pages"],
            "all_extractions_complete": True,
            "checkpoint_key": CHECKPOINT_KEY,
            "media_watermark_key": MEDIA_WATERMARK_KEY,
        })
        return response_for(state, 200, "Extraction complete", processed)

    return response_for(state, 202, "More pages remain; invoke again", processed)
