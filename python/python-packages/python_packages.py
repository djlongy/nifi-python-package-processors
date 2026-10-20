"""The rules PlanPythonTransfer and RoutePythonTransfer apply, kept free of NiFi imports so they read and test alone.

Both processors only read metadata: the small JSON request and FlowFile attributes. Native InvokeHTTP downloads and
uploads every wheel and CryptographicHashContent hashes it, so a wheel's bytes never pass through Python.
"""
import base64
import hashlib
import html.parser
import json
import re
import urllib.parse
import urllib.request

WHEEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+!-]*[.]whl")
SHA256 = re.compile(r"[0-9a-f]{64}")

REQUEST_TYPE, REQUEST_FORMATS = "python-packages-request", ("json", "text")
WHEEL_TYPE, WHEEL_FORMAT, WHEEL_ACTION = "python-packages", "whl", "mirror"
FIELDS = ("filename", "name", "version", "sha256", "size", "url")


class TransferError(Exception):
    """A request or wheel that must not go on; the message says why and lands in pypi.error."""


def plan_jobs(body, attributes, allowed_base, max_files, lookup=None):
    """One job per wheel the request names.

    X-Artifact-Format json: the mirror's request body {"registry", "files": [{filename, name, version, sha256, size,
    url}]}, with X-Sha256 of the body. X-Artifact-Format text (a resend typed into GenerateFlowFile): one wheel file
    name per line, # comments allowed; lookup(filename) finds its url and sha256 in the low index, and X-Sha256 is
    checked only when given.

    The whole request is refused when its headers, its X-Sha256 or its body are wrong. A single wheel that fails the
    allowlist is kept as a job with an "error", so RoutePythonTransfer rejects that wheel alone and the rest still go.
    """
    kind = attributes.get("X-Artifact-Format")
    if attributes.get("X-Artifact-Type") != REQUEST_TYPE or kind not in REQUEST_FORMATS:
        raise TransferError(f"not a Python package request: X-Artifact-Type {attributes.get('X-Artifact-Type')!r}, "
                            f"X-Artifact-Format {kind!r}")
    digest = (attributes.get("X-Sha256") or "").lower()
    if (digest or kind == "json") and hashlib.sha256(body).hexdigest() != digest:
        raise TransferError("request body does not match its X-Sha256")
    files = text_files(body, lookup) if kind == "text" else json_files(body)
    if len(files) > max_files:
        raise TransferError(f"{len(files)} files is more than Maximum Files ({max_files})")
    jobs = []
    for entry in files:
        job = {key: str(entry.get(key) or "") for key in FIELDS} if isinstance(entry, dict) else dict.fromkeys(FIELDS, "")
        error = entry_error(job, allowed_base)
        jobs.append({**job, "error": error} if error else job)
    return jobs


def json_files(body):
    try:
        files = json.loads(body)["files"]
    except (ValueError, KeyError, TypeError) as error:
        raise TransferError(f"request is not JSON with a files list: {error}") from None
    if not isinstance(files, list):
        raise TransferError("files is not a list")
    return files


def text_files(body, lookup):
    """The entries for a list of wheel file names, each looked up in the index."""
    if lookup is None:
        raise TransferError("a text request needs Index URL set on PlanPythonTransfer")
    try:
        lines = body.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        raise TransferError("text request is not UTF-8") from None
    names = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
    if not names:
        raise TransferError("text request names no wheel")
    return [lookup(name) for name in names]


def wheel_name_version(filename):
    """(distribution, version) from a wheel file name, or ("", "")."""
    parts = filename[:-len(".whl")].split("-") if WHEEL.fullmatch(filename) else []
    return (parts[0], parts[1]) if len(parts) >= 5 else ("", "")


class _Links(html.parser.HTMLParser):
    """The <a href> values of a PEP 503 simple page."""

    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.hrefs += [value for key, value in attrs if key == "href" and value]


def index_lookup(index_url, username="", password="", timeout=60, opener=None):
    """lookup(filename) for text requests: reads <index>/simple/<project>/ (PEP 503, as GitLab, PyPI, pypiserver,
    Artifactory and Nexus serve it) and returns the entry for that file, its sha256 from the link's #sha256=.
    A file the index does not list comes back with an error the allowlist reports."""
    opener = opener or urllib.request.build_opener()
    pages = {}

    def page(project):
        if project not in pages:
            url = f"{index_url.rstrip('/')}/simple/{project}/"
            request = urllib.request.Request(url, headers={"Accept": "text/html"})
            if username or password:
                token = f"{username}:{password}".encode()
                request.add_header("Authorization", "Basic " + base64.b64encode(token).decode())
            try:
                with opener.open(request, timeout=timeout) as response:
                    links = _Links()
                    links.feed(response.read(4 << 20).decode("utf-8", "replace"))
            except OSError as error:
                raise TransferError(f"index {url}: {error}") from None
            pages[project] = {}
            for href in links.hrefs:
                target, _, fragment = urllib.parse.urljoin(url, href).partition("#")
                pages[project][urllib.parse.unquote(target.rsplit("/", 1)[-1])] = (
                    target, fragment[len("sha256="):] if fragment.startswith("sha256=") else "")
        return pages[project]

    def lookup(filename):
        name, version = wheel_name_version(filename)
        if not name:
            return {"filename": filename, "name": "", "version": "", "sha256": "", "size": "", "url": ""}
        url, sha256 = page(re.sub(r"[-_.]+", "-", name).lower()).get(filename, ("", ""))
        return {"filename": filename, "name": name, "version": version, "sha256": sha256, "size": "", "url": url}

    return lookup


def entry_error(job, allowed_base):
    """Why a job may not be fetched, or "" when it may: the wheel must come from the low registry's files URL."""
    if not job["url"]:
        return f"{job['filename']!r} is not in the index"
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
