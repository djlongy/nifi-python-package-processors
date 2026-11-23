# python-packages: NiFi processors that move Python packages across a data diode

This folder holds two Apache NiFi 2 Python processors, **PlanPythonTransfer** and
**RoutePythonTransfer**. Together with NiFi's own InvokeHTTP they copy Python wheels
(and, if allowed, source distributions) from a package index on one network to a
package index on another, for example across a one-way data diode.

The processors only read metadata: a small JSON request and FlowFile attributes. NiFi's
native **InvokeHTTP** downloads and uploads every file, and **CryptographicHashContent**
hashes it, so a package's bytes never pass through Python memory. A 414 MB wheel crosses
like a 10 KB one.

## What each processor does

| Processor | Side | Job |
|---|---|---|
| PlanPythonTransfer | low (source) | Checks a transfer request and turns it into one job per file. A file whose URL is outside the allowed base, or whose name or sha256 is malformed, is refused on its own; the rest still go. |
| RoutePythonTransfer | both | Decides the next step for each file. Low side: sends it to InvokeHTTP to download, then labels it for the diode once its SHA-256 matches. High side: checks it again, sends it to InvokeHTTP to upload, and reads the index's answer. A file the index already holds counts as done. |

The request usually comes from a package mirror job, one JSON POST per batch, naming each
file's name, version, sha256, size and URL. Any index that serves files by URL and accepts
the standard `twine` upload API works: GitLab, PyPI, pypiserver, Artifactory and Nexus.

## Requirements

- Apache NiFi 2.x with Python processors enabled, Python 3.10 or later on each node.
- Nothing from PyPI: the code uses only the Python standard library.

## Install

Copy this **whole folder**, helper modules included (`python_packages.py`,
`nifi_attributes.py`, `__init__.py`), into NiFi's Python extensions directory on every
node, then restart NiFi:

```sh
cp -R python-packages /opt/nifi/nifi-current/python_extensions/
```

Copying only the two processor files is not enough: they import the helpers.

## Files

| File | Purpose |
|---|---|
| `PlanPythonTransfer.py` | The planning processor (low side). |
| `RoutePythonTransfer.py` | The routing processor (both sides). |
| `python_packages.py` | Shared checks: file names, hashes, allowed URLs, request parsing. |
| `nifi_attributes.py` | Names of the FlowFile attributes both processors read and write. |

## More

Flows, properties, attributes, TLS notes and a step-by-step build guide with screenshots:
<https://github.com/djlongy/nifi-python-package-processors>
