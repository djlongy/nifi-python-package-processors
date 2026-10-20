"""The rules PlanPythonTransfer and RoutePythonTransfer apply, kept free of NiFi imports so they read and test alone.

Both processors only read metadata: the small JSON request and FlowFile attributes. Native InvokeHTTP downloads and
uploads every wheel and CryptographicHashContent hashes it, so a wheel's bytes never pass through Python.
"""
import base64
import hashlib
import hmac
import html.parser
import json
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request

WHEEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+!-]*[.]whl")
# A source distribution: name-version.tar.gz or .zip (PEP 625, and older names with hyphens in the project).
SDIST = re.compile(r"([A-Za-z0-9][A-Za-z0-9._+!-]*)-([0-9][A-Za-z0-9._+!]*)[.](?:tar[.]gz|zip)")
SHA256 = re.compile(r"[0-9a-f]{64}")

REQUEST_TYPE, REQUEST_FORMATS = "python-packages-request", ("json", "text")
WHEEL_TYPE, WHEEL_FORMAT, WHEEL_ACTION = "python-packages", "whl", "mirror"
SDIST_FORMAT = "sdist"
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
    """(distribution, version) from a wheel or source distribution file name, or ("", "")."""
    sdist = SDIST.fullmatch(filename)
    if sdist:
        return sdist.groups()
    parts = filename[:-len(".whl")].split("-") if WHEEL.fullmatch(filename) else []
    return (parts[0], parts[1]) if len(parts) >= 5 else ("", "")


def package_format(filename):
    """X-Artifact-Format for a package file: whl, sdist, or "" for anything else."""
    return WHEEL_FORMAT if WHEEL.fullmatch(filename) else SDIST_FORMAT if SDIST.fullmatch(filename) else ""


class _Links(html.parser.HTMLParser):
    """The <a href> values of a PEP 503 simple page."""

    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.hrefs += [value for key, value in attrs if key == "href" and value]


def trusted_der(property_value):
    """The DER bytes of every CA in the SSL Context Service property_value names, [] when it is unset.
    py4j hands getEncoded() over as a Java byte[], which bytes() converts."""
    if not property_value.getValue():
        return []
    manager = property_value.asControllerService().createTrustManager()
    if manager is None:
        raise TransferError("the SSL Context Service has no truststore")
    return [bytes(issuer.getEncoded()) for issuer in manager.getAcceptedIssuers()]


def trust_context(ca_der=()):
    """The system store (and SSL_CERT_FILE) plus the SSL Context Service's CAs. Verification is never turned off."""
    context = ssl.create_default_context()
    for der in ca_der:
        context.load_verify_locations(cadata=bytes(der))
    if ca_der:
        # Python 3.13 sets VERIFY_X509_STRICT, which rejects a private CA without the key usage extension
        # (a quick `openssl req -x509` CA). Trust still comes only from the system store and these CAs.
        context.verify_flags &= ~getattr(ssl, "VERIFY_X509_STRICT", 0)
    return context


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect is an error: the index must answer itself, so credentials never follow a Location elsewhere."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, f"redirect to {newurl} refused", headers, fp)


def web_opener(context):
    """http and https only (no file: or ftp:), with this TLS context, and no redirects."""
    opener = urllib.request.OpenerDirector()
    for handler in (urllib.request.HTTPHandler(), urllib.request.HTTPSHandler(context=context),
                    urllib.request.HTTPErrorProcessor(), urllib.request.HTTPDefaultErrorHandler(), _NoRedirect()):
        opener.add_handler(handler)
    return opener


def check_index_url(index_url):
    """index_url, or TransferError when it is not an http(s) URL without credentials."""
    parts = urllib.parse.urlsplit(index_url or "")
    if parts.scheme not in ("https", "http") or not parts.netloc:
        raise TransferError(f"Index URL must start with https:// or http://: {index_url!r}")
    if "@" in parts.netloc:
        raise TransferError("Index URL must not carry a user name or password; set Index Username and Index Password")
    return index_url.rstrip("/")


def same_origin(link, index_url):
    """A link under the index's own path, on the index's scheme and host. GitLab writes its external_url into
    the simple page, which need not be the address NiFi reaches it on, so the allowlist would refuse every link.
    A link elsewhere is returned as it is, for the allowlist to refuse."""
    target, index = urllib.parse.urlsplit(link), urllib.parse.urlsplit(index_url)
    if target.path.startswith(index.path.rstrip("/") + "/"):
        return urllib.parse.urlunsplit((index.scheme, index.netloc, target.path, target.query, ""))
    return link


def index_lookup(index_url, username="", password="", timeout=60, opener=None):
    """lookup(filename) for text requests: reads <index>/simple/<project>/ (PEP 503, as GitLab, PyPI, pypiserver,
    Artifactory and Nexus serve it) and returns the entry for that file, its sha256 from the link's #sha256=.
    A file the index does not list comes back with an error the allowlist reports."""
    index_url = check_index_url(index_url)
    opener = opener or web_opener(trust_context())
    pages = {}

    def page(project):
        if project not in pages:
            url = f"{index_url}/simple/{project}/"
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
                target = same_origin(target, index_url)
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


# Anything InvokeHTTP could resolve to another path than the one the prefix check saw.
UNSAFE_URL = re.compile(r"%2e|%2f|%5c|\\|[?#]", re.IGNORECASE)


def url_outside(url, allowed_base):
    """True when url could reach anything but a file under allowed_base: another scheme or host, a dot segment
    (../ or ./), an encoded dot, slash or backslash, a query or a fragment."""
    parts, base = urllib.parse.urlsplit(url), urllib.parse.urlsplit(allowed_base)
    return (not url.startswith(allowed_base) or (parts.scheme, parts.netloc) != (base.scheme, base.netloc)
            or bool(UNSAFE_URL.search(url)) or any(segment in (".", "..") for segment in parts.path.split("/")))


def entry_error(job, allowed_base):
    """Why a job may not be fetched, or "" when it may: the wheel must come from the low registry's files URL."""
    if not job["url"]:
        return f"{job['filename']!r} is not in the index"
    if url_outside(job["url"], allowed_base):
        return f"{job['filename'] or job['url']!r} is not under {allowed_base}"
    if not package_format(job["filename"]):
        return f"{job['filename']!r} is not a wheel or source distribution file name"
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
            "X-Artifact-Type": WHEEL_TYPE, "X-Artifact-Format": package_format(attributes["pypi.filename"]),
            "X-Artifact-Action": WHEEL_ACTION,
            "mime.type": "application/octet-stream", "pypi.stage": "sent"}


def upload_key(secret, uuid, filename):
    """What the high RoutePythonTransfer stamps on a wheel it sends to upload: only it knows secret, so a FlowFile
    whose pypi.stage and invokehttp.* came across the diode cannot pass as an answer from the upload."""
    return hmac.new(secret, f"{uuid}\n{filename}".encode(), hashlib.sha256).hexdigest()


def arrived_attributes(attributes, secret=None):
    """High side: a wheel from the diode may be uploaded when its labels are a wheel's and its bytes match X-Sha256.
    Name and version come from the file name, never from attributes set on the low side, and the answer of any
    earlier request is cleared so it cannot be read as this upload's."""
    kind = package_format(attributes.get("filename") or "")
    if not kind:
        raise TransferError(f"{attributes.get('filename')!r} is not a wheel or source distribution file name")
    if attributes.get("X-Artifact-Type") != WHEEL_TYPE or attributes.get("X-Artifact-Format") != kind:
        raise TransferError(f"{attributes.get('filename')} is not labelled a Python {kind}")
    if attributes.get("content_SHA-256") != attributes.get("X-Sha256"):
        raise TransferError(f"{attributes.get('filename')} does not match its X-Sha256")
    name, version = wheel_name_version(attributes["filename"])
    stamp = {"pypi.upload.key": upload_key(secret, attributes.get("uuid") or "", attributes["filename"])} if secret else {}
    return {"pypi.stage": "upload", "pypi.name": name, "pypi.version": version, "pypi.response": "",
            "pypi.filetype": "sdist" if kind == SDIST_FORMAT else "bdist_wheel",
            "invokehttp.status.code": "", "invokehttp.request.url": ""} | stamp


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
