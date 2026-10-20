# nifi-python-package-processors

Two NiFi 2 Python processors that move Python wheels across a data diode from one package index to another: **PlanPythonTransfer** and **RoutePythonTransfer**. They only read metadata (the small JSON request and FlowFile attributes). Native **InvokeHTTP** downloads and uploads every wheel and **CryptographicHashContent** hashes it, so a wheel's bytes never pass through Python memory, however large.

The request comes from the request job of [gitlab-pypi-mirror](https://github.com/djlongy/gitlab-pypi-mirror): one JSON POST naming each wheel's filename, name, version, sha256, size and url. The processors work with any index that serves files by URL and accepts the standard upload API that `twine` uses: GitLab, PyPI, pypiserver, Artifactory and Nexus.

The processors are plain Python with no dependencies outside the standard library, so anyone can read, review and change them.

| Processor | Side | What it does |
|---|---|---|
| PlanPythonTransfer | low | Checks the request (`X-Artifact-Type python-packages-request`, `X-Artifact-Format json`, `X-Sha256` of the body) and emits `{"jobs": [...]}`, one job per wheel. A resend typed into GenerateFlowFile uses `X-Artifact-Format text` and one wheel file name per line; each is looked up in **Index URL**'s `/simple/<project>/` page for its url and sha256. A wheel whose url is not under **Allowed URL Base**, or whose filename or sha256 is malformed, keeps an `error` and is rejected alone; the rest still go. |
| RoutePythonTransfer | both | Decides each step from `pypi.stage`. Low: sends a job to InvokeHTTP GET, then labels the fetched wheel for the diode once its sha256 matches. High: checks the wheel from the diode before InvokeHTTP POST, then reads the index's answer. A file the index already holds counts as done. |

Anything refused goes to `failure` with `pypi.error` saying why.

## Flows

Low side, 8 processors:

```
ListenHTTP -> PlanPythonTransfer -> SplitJson ($.jobs) -> RoutePythonTransfer (Low)
RoutePythonTransfer request -> InvokeHTTP GET ${pypi.url} (Response) -> CryptographicHashContent -> RoutePythonTransfer
RoutePythonTransfer success -> PackageFlowFile -> to diode
every failure, InvokeHTTP No Retry and Failure -> LogMessage "Rejected (inspect queue)", kept stopped
```

High side, 5 processors:

```
from diode -> UnpackContent (flowfile-stream-v3) -> CryptographicHashContent -> RoutePythonTransfer (High)
RoutePythonTransfer request -> InvokeHTTP POST (Original, No Retry) -> RoutePythonTransfer
RoutePythonTransfer success: auto-terminated (pypi.result uploaded or duplicate)
every failure and InvokeHTTP Failure -> LogMessage "Rejected (inspect queue)", kept stopped
```

InvokeHTTP retries its own `Retry` relationship in both flows. Keep the default back pressure (10000) on every connection in a loop.

## Properties

| Processor | Property | Default | Meaning |
|---|---|---|---|
| PlanPythonTransfer | Allowed URL Base | none | Every wheel url must start with this, such as `#{low.pypi.url}/files/` for GitLab. Never taken from the request. |
| PlanPythonTransfer | Maximum Files | `1000` | Most wheels one request may name. The mirror sends 200 per request. |
| PlanPythonTransfer | Index URL | none | Text requests only: the low index, such as `#{low.pypi.url}`; its PEP 503 `/simple/` pages give each wheel's url and sha256. Empty refuses text requests. |
| PlanPythonTransfer | Index Username, Index Password | none | Credentials for Index URL; the password as a sensitive parameter. |
| RoutePythonTransfer | Transfer Side | `Low` | `Low` or `High`. |
| RoutePythonTransfer | Upload URL | none | High: exactly the upload InvokeHTTP's HTTP URL. An answer from any other URL is refused, so attributes carried across the diode cannot pass as an upload. |
| RoutePythonTransfer | Already Uploaded Pattern | GitLab, PyPI, pypiserver, Artifactory, Nexus | High: a regular expression on `<HTTP status> <response body>` meaning the index already holds the file. |

## Resend from NiFi

Add a GenerateFlowFile on the low side, connected to PlanPythonTransfer, and keep it stopped. To resend, set its Custom Text to the wheel file names, one per line, and dynamic properties `X-Artifact-Type` = `python-packages-request` and `X-Artifact-Format` = `text`, then **Run Once**. Each name is looked up in Index URL and goes through the same allowlist, fetch and sha256 checks as a request from the pipeline.

## InvokeHTTP settings

| Side | Property | Value |
|---|---|---|
| Low | HTTP Method / HTTP URL | `GET` / `${pypi.url}` |
| Low | Request Username / Password | the low index's read account, as parameters |
| High | HTTP Method / HTTP URL | `POST` / the same value as Upload URL |
| High | Request Username / Password | the high index's upload account, as parameters |
| High | Request Multipart Form-Data Name | `content` (filename enabled) |
| High | Request Content-Type | `${mime.type}` |
| High | dynamic `post:form:name`, `post:form:version`, `post:form:sha256_digest` | `${pypi.name}`, `${pypi.version}`, `${content_SHA-256}` |
| High | dynamic `post:form::action`, `post:form:protocol_version` | `file_upload`, `1` (PyPI and pypiserver need them; GitLab ignores them) |
| High | Response Body Attribute Name / Size | `pypi.response` / `256` |

Credentials stay on InvokeHTTP as sensitive parameters. They are never written to an attribute.

## Install

Copy the whole `python/python-packages` directory into NiFi's Python extensions directory on every node. NiFi discovers it without a restart. For containers, build `python/Containerfile` and copy `/extensions/python-packages` from an init container, as the container-registry processors do.

## Test

```sh
python3 -m unittest discover -s python/tests -v
ruff check python
```

`python/tests/nifi_stub.py` stands in for NiFi's `nifiapi` so the processors run without NiFi.
