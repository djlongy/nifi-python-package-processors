# nifi-python-package-processors

Two NiFi 2 Python processors that move Python wheels across a data diode from one package index to another: **PlanPythonTransfer** and **RoutePythonTransfer**. They read only metadata: the small JSON request and FlowFile attributes. Native **InvokeHTTP** downloads and uploads every wheel and **CryptographicHashContent** hashes it, so a wheel's bytes never pass through Python memory, however large (a 414 MB pyspark wheel crosses like a 10 KB one).

The request comes from the request job of [gitlab-pypi-mirror](https://github.com/djlongy/gitlab-pypi-mirror): one JSON POST per batch naming each wheel's filename, name, version, sha256, size and url. The processors work with any index that serves files by URL and takes the standard upload API `twine` uses: GitLab, PyPI, pypiserver, Artifactory and Nexus.

They are plain Python with nothing outside the standard library, so anyone can read, review and change them.

| Processor | Side | What it does |
|---|---|---|
| PlanPythonTransfer | low | Checks the request and emits `{"jobs": [...]}`, one job per wheel, for SplitJson. A wheel whose url is not under **Allowed URL Base**, or whose filename or sha256 is malformed, keeps an `error` and is refused alone; the rest still go. A resend typed into GenerateFlowFile names wheel files, which it looks up in **Index URL**. |
| RoutePythonTransfer | both | Decides each step from `pypi.stage`. Low: sends a job to InvokeHTTP GET, then labels the fetched wheel for the diode once its SHA-256 matches. High: checks the wheel from the diode before InvokeHTTP POST, then reads the index's answer; a file the index already holds counts as done. |

## Flows

Low side, 7 processors and the `to diode` port:

```
ListenHTTP -> PlanPythonTransfer -> SplitJson ($.jobs) -> RoutePythonTransfer (Low)
RoutePythonTransfer request -> InvokeHTTP GET ${pypi.url} (Response) -> CryptographicHashContent -> RoutePythonTransfer
RoutePythonTransfer success -> to diode
every failure, InvokeHTTP No Retry and Failure -> LogMessage "Rejected (inspect queue)", kept stopped
GenerateFlowFile "Resend" (stopped, Run Once) -> PlanPythonTransfer
```

High side, 4 processors after the `from diode` port:

```
from diode -> CryptographicHashContent -> RoutePythonTransfer (High)
RoutePythonTransfer request -> InvokeHTTP POST (Original, No Retry) -> RoutePythonTransfer
RoutePythonTransfer success: auto-terminated (pypi.result uploaded or duplicate)
every failure and InvokeHTTP Failure -> LogMessage "Rejected (inspect queue)", kept stopped
```

The diode carries each FlowFile with its attributes (FlowFile Stream v3 at the link), so neither side packs or unpacks. InvokeHTTP retries its own `Retry` relationship. Keep the default back pressure (10000) on every connection in a loop.

## Relationships

| Processor | Relationship | Meaning |
|---|---|---|
| PlanPythonTransfer | success | One JSON job list, `mime.type` `application/json`, with `pypi.job.count` and `pypi.refused.count`. Connect to SplitJson on `$.jobs`. |
| PlanPythonTransfer | failure | The whole request was refused (headers, X-Sha256, body, size or index); `pypi.error` says why and nothing is fetched. |
| RoutePythonTransfer | request | Low: a job ready for InvokeHTTP GET. High: a checked wheel ready for InvokeHTTP POST. |
| RoutePythonTransfer | success | Low: a verified, labelled wheel for the diode. High: uploaded or already there (`pypi.result` `uploaded` or `duplicate`). |
| RoutePythonTransfer | failure | Refused: allowlist, sha256 mismatch, not a wheel, or rejected upload; `pypi.error` says why. |

## Attributes

| Attribute | Set by | Meaning |
|---|---|---|
| `pypi.filename`, `pypi.name`, `pypi.version`, `pypi.sha256`, `pypi.size`, `pypi.url` | Route (low) | The job; InvokeHTTP fetches `${pypi.url}`, the high InvokeHTTP posts `${pypi.name}` and `${pypi.version}` |
| `filename`, `X-Sha256`, `X-Artifact-Type` `python-packages`, `X-Artifact-Format` `whl`, `X-Artifact-Action` `mirror`, `mime.type` | Route (low, after the hash) | The labels the wheel crosses with; route on `X-Artifact-Type` on the high side |
| `pypi.stage` | Route | `fetch`, `sent`, `upload`: which step a FlowFile is at |
| `pypi.upload.key` | Route (high) | An HMAC over the FlowFile's uuid and filename, keyed by a secret only this RoutePythonTransfer holds. An `upload` stage without it, or with one from the low side, is checked and uploaded again, so attributes carried across the diode never pass as an upload answer |
| `pypi.result` | Route (high) | `uploaded` or `duplicate` |
| `pypi.error` | both | Why a FlowFile went to failure |
| `pypi.job.count`, `pypi.refused.count` | Plan | Wheels in the request, and how many the allowlist refused |

On the high side, `pypi.name` and `pypi.version` are read again from the wheel's file name, and `pypi.response` and `invokehttp.*` are cleared before the upload: nothing from the low side decides what is uploaded or how the answer is read. A wheel url must start with Allowed URL Base and also keep its scheme and host, with no `.` or `..` segment, no encoded dot, slash or backslash, and no query or fragment.

The request's own headers (`X-Registry`, `X-CI-Project`, `X-CI-Pipeline-Url`, `X-CI-Commit-Sha`, `X-File-Count`) stay on every wheel from it, for audit.

## Properties

### PlanPythonTransfer

<!-- properties:PlanPythonTransfer -->
| Name | Required | Default | Description |
|---|---|---|---|
| Allowed URL Base | Yes | none | Every wheel url must start with this, such as https://gitlab.example.com/api/v4/projects/12/packages/pypi/files/ or #{low.pypi.url}/files/. Never taken from the request. Required. Environment variables allowed. |
| Maximum Files | Yes | `1000` | Most wheels one request may name; more refuses the whole request. Default 1000. The mirror sends 200. |
| Index URL | No | none | Text requests only: the low index, such as https://gitlab.example.com/api/v4/projects/12/packages/pypi, whose /simple/<project>/ pages give each wheel's url and sha256. No user:password in the URL. Empty (default): text requests are refused. Environment variables allowed. |
| Index Username | No | none | User for Index URL, such as __token__ for a GitLab token. Empty (default): anonymous. Environment variables allowed. |
| Index Password | No | none | Password or token for Index URL. Empty (default): none sent. Sensitive: reference a parameter, such as #{low.pypi.token}. |
| SSL Context Service | No | none | A StandardSSLContextService whose PKCS12 or JKS truststore holds the index's CA (it does not accept PEM). Its CAs add to the system store and SSL_CERT_FILE. Empty (default): only those are trusted. Its keystore is ignored. |
| Request Timeout | Yes | `60` | Seconds an index page read may stall before the request fails. Default 60. Wheels are never read here. |
<!-- /properties -->

### RoutePythonTransfer

<!-- properties:RoutePythonTransfer -->
| Name | Required | Default | Description |
|---|---|---|---|
| Transfer Side | Yes | `Low` | Low (default): prepares downloads and labels verified wheels. High: checks wheels and reads upload answers. |
| Upload URL | No | none | High side, required there: exactly the upload InvokeHTTP's HTTP URL, such as #{high.pypi.url}. An answer from any other URL is refused, so attributes carried across the diode cannot pass as an upload. Environment variables allowed. |
| Already Uploaded Pattern | Yes | `^(400\|409) .*(already been taken\|already exists\|does not allow updating)` | High side: a regular expression on "<HTTP status> <response body>" (InvokeHTTP Response Body Attribute Name pypi.response) meaning the index already holds the file, so it counts as done. The default covers GitLab, PyPI, pypiserver, Artifactory and Nexus. |
<!-- /properties -->

## TLS and private CAs

Both processors and InvokeHTTP verify certificates; nothing turns verification off.

- **PlanPythonTransfer** (index lookups for a resend) trusts the system store, `SSL_CERT_FILE`, and the CAs of its **SSL Context Service**.
- **InvokeHTTP** trusts Java's default CAs while its SSL Context Service is empty. Once one is selected, it trusts that truststore only, so the truststore must hold every CA in the index's chain.

For a self-signed or private CA, create one **StandardSSLContextService** (Truststore Filename `/opt/nifi/certs/index-trust.p12`, Truststore Password `#{truststore_password}`, Truststore Type `PKCS12`), enable it, and select it on PlanPythonTransfer and on both InvokeHTTP (fetch on the low side, upload on the high side). Build the truststore on every node:

```sh
keytool -importcert -noprompt -alias index-ca -file /path/to/index-ca.pem \
  -keystore /opt/nifi/certs/index-trust.p12 -storetype PKCS12
```

Import an intermediate CA under its own alias. RoutePythonTransfer makes no connection, so it has no SSL property.

## InvokeHTTP settings

| Side | Property | Value |
|---|---|---|
| Low | HTTP Method / HTTP URL | `GET` / `${pypi.url}` |
| Low | Request Username / Password | the low index's read account, as parameters |
| High | HTTP Method / HTTP URL | `POST` / the same value as Upload URL |
| High | Request Username / Password | the high index's upload account, as parameters |
| High | Request Multipart Form-Data Name | `content` (Request Multipart Form-Data Filename Enabled `true`) |
| High | Request Content-Type | `${mime.type}` |
| High | dynamic `post:form:name`, `post:form:version`, `post:form:sha256_digest` | `${pypi.name}`, `${pypi.version}`, `${content_SHA-256}` |
| High | dynamic `post:form::action`, `post:form:protocol_version` | `file_upload`, `1` (PyPI and pypiserver need them; GitLab ignores them) |
| High | Response Body Attribute Name / Size | `pypi.response` / `256` |
| Both | SSL Context Service | the StandardSSLContextService above, for a private CA |

Credentials stay on InvokeHTTP as sensitive parameters. They are never written to an attribute.

## Resend from NiFi

The pipeline resends with `MIRROR_RESEND_PACKAGES`. From NiFi, without the pipeline: a stopped **GenerateFlowFile** named `Resend` connected to PlanPythonTransfer, with

| Property | Value |
|---|---|
| Custom Text | the wheel file names, one per line, such as `six-1.17.0-py2.py3-none-any.whl` |
| dynamic `X-Artifact-Type` | `python-packages-request` |
| dynamic `X-Artifact-Format` | `text` |

Right-click, **Run Once**. Each name is looked up in Index URL and then goes through the same allowlist, fetch and SHA-256 checks as a request from the pipeline. A link the index page gives under Index URL's path is read from Index URL's host, since GitLab writes its `external_url` into the page, which need not be the address NiFi uses.

## Install

Copy the whole `python/python-packages` directory into NiFi's Python extensions directory on every node, then restart NiFi: it discovers Python processors at start. For containers, build `python/Containerfile` and copy `/extensions/python-packages` from an init container, as the container-registry processors do (`infra/nifi-python-extensions`).

## Test

```sh
python3 -m unittest discover -s python/tests -v
ruff check python
python3 python/tests/test_readme_tables.py --write   # after changing a property
```

`python/tests/nifi_stub.py` stands in for NiFi's `nifiapi`, so the processors run without NiFi.
