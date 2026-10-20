"""The rules PlanPythonTransfer and RoutePythonTransfer apply, kept free of NiFi imports so they read and test alone.

Both processors only read metadata: the small JSON request and FlowFile attributes. Native InvokeHTTP downloads and
uploads every wheel and CryptographicHashContent hashes it, so a wheel's bytes never pass through Python.
"""
import hashlib
import json
import re

WHEEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+!-]*[.]whl")
SHA256 = re.compile(r"[0-9a-f]{64}")

REQUEST_TYPE, REQUEST_FORMAT = "python-packages-request", "json"
WHEEL_TYPE, WHEEL_FORMAT, WHEEL_ACTION = "python-packages", "whl", "mirror"
FIELDS = ("filename", "name", "version", "sha256", "size", "url")


class TransferError(Exception):
    """A request or wheel that must not go on; the message says why and lands in pypi.error."""


def plan_jobs(body, attributes, allowed_base, max_files):
    """One job per entry of a request body {"registry", "files": [...]}.

    The whole request is refused when its headers, its X-Sha256 or its JSON are wrong. A single entry that fails the
    allowlist is kept as a job with an "error", so RoutePythonTransfer rejects that wheel alone and the rest still go.
    """
    if attributes.get("X-Artifact-Type") != REQUEST_TYPE or attributes.get("X-Artifact-Format") != REQUEST_FORMAT:
        raise TransferError(f"not a Python package request: X-Artifact-Type {attributes.get('X-Artifact-Type')!r}, "
                            f"X-Artifact-Format {attributes.get('X-Artifact-Format')!r}")
    if hashlib.sha256(body).hexdigest() != (attributes.get("X-Sha256") or "").lower():
        raise TransferError("request body does not match its X-Sha256")
    try:
        files = json.loads(body)["files"]
    except (ValueError, KeyError, TypeError) as error:
        raise TransferError(f"request is not JSON with a files list: {error}") from None
    if not isinstance(files, list):
        raise TransferError("files is not a list")
    if len(files) > max_files:
        raise TransferError(f"{len(files)} files is more than Maximum Files ({max_files})")
    jobs = []
    for entry in files:
        job = {key: str(entry.get(key) or "") for key in FIELDS} if isinstance(entry, dict) else dict.fromkeys(FIELDS, "")
        error = entry_error(job, allowed_base)
        jobs.append({**job, "error": error} if error else job)
    return jobs


def entry_error(job, allowed_base):
    """Why a job may not be fetched, or "" when it may: the wheel must come from the low registry's files URL."""
    if not job["url"].startswith(allowed_base):
        return f"{job['filename'] or job['url']!r} is not under {allowed_base}"
    if not WHEEL.fullmatch(job["filename"]):
        return f"{job['filename']!r} is not a wheel file name"
    if not SHA256.fullmatch(job["sha256"]):
        return f"{job['filename']} has no sha256 of 64 lowercase hex characters"
    return ""


def fetch_attributes(job):
    """The attributes InvokeHTTP fetches a job's wheel with: HTTP URL is ${pypi.url}."""
    if job.get("error"):
        raise TransferError(job["error"])
    return {f"pypi.{key}": job[key] for key in FIELDS} | {"pypi.stage": "fetch"}


def fetched_attributes(attributes):
    """The labels a fetched wheel crosses the diode with, once its bytes match the sha256 the request named."""
    if attributes.get("invokehttp.request.url") != attributes.get("pypi.url"):
        raise TransferError(f"{attributes.get('pypi.filename')} was not fetched from its pypi.url")
    if attributes.get("content_SHA-256") != attributes.get("pypi.sha256"):
        raise TransferError(f"{attributes.get('pypi.filename')} does not match its sha256")
    return {"filename": attributes["pypi.filename"], "X-Sha256": attributes["pypi.sha256"],
            "X-Artifact-Type": WHEEL_TYPE, "X-Artifact-Format": WHEEL_FORMAT, "X-Artifact-Action": WHEEL_ACTION,
            "mime.type": "application/octet-stream", "pypi.stage": "sent"}


def arrived_attributes(attributes):
    """High side: a wheel from the diode may be uploaded when its labels are a wheel's and its bytes match X-Sha256."""
    if attributes.get("X-Artifact-Type") != WHEEL_TYPE or attributes.get("X-Artifact-Format") != WHEEL_FORMAT:
        raise TransferError(f"{attributes.get('filename')} is not labelled a Python wheel")
    if not WHEEL.fullmatch(attributes.get("filename") or ""):
        raise TransferError(f"{attributes.get('filename')!r} is not a wheel file name")
    if attributes.get("content_SHA-256") != attributes.get("X-Sha256"):
        raise TransferError(f"{attributes.get('filename')} does not match its X-Sha256")
    return {"pypi.stage": "upload"}


# What GitLab (400 "has already been taken"), PyPI/Warehouse (400 "File already exists"), Nexus (400 "does not
# allow updating assets") and pypiserver or Artifactory (409 "... already exists") answer for a file already held.
ALREADY_THERE = r"^(400|409) .*(already been taken|already exists|does not allow updating)"


def upload_result(attributes, upload_url, already_there=ALREADY_THERE):
    """High side, after InvokeHTTP: "uploaded" on 2xx, "duplicate" when "<status> <response>" matches already_there."""
    if attributes.get("invokehttp.request.url") != upload_url:
        raise TransferError(f"{attributes.get('filename')} was not uploaded to {upload_url}")
    status, response = attributes.get("invokehttp.status.code") or "", attributes.get("pypi.response") or ""
    if status.startswith("2"):
        return "uploaded"
    if re.search(already_there, f"{status} {response}", re.IGNORECASE | re.DOTALL):
        return "duplicate"
    raise TransferError(f"{attributes.get('filename')} upload failed: HTTP {status or 'none'} {response}".strip())
