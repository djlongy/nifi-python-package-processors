import hmac
import json
import os

from nifiapi.documentation import use_case
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import ExpressionLanguageScope, PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship

from nifi_attributes import Attributes, evaluate
from python_packages import (ALREADY_THERE, WHEEL_FORMAT, TransferError, arrived_attributes, fetch_attributes,
                             fetched_attributes, format_refused, upload_key, upload_result)

ERROR, STAGE = "pypi.error", "pypi.stage"
READ = ("pypi.filename", "pypi.url", "pypi.sha256", "filename", "X-Sha256", "X-Artifact-Type", "X-Artifact-Format",
        "content_SHA-256", "invokehttp.request.url", "invokehttp.response.url", "invokehttp.status.code", "pypi.response", "uuid", "pypi.upload.key",
        "pypi.requires_python", "X-Requires-Python")


@use_case(description="Low side: fetch each wheel a PlanPythonTransfer job names with native InvokeHTTP and label it for "
                      "the diode once its bytes match the requested sha256.",
          configuration="""SplitJson ($.jobs) -> this processor (Transfer Side Low). request -> InvokeHTTP (GET, HTTP URL
          ${pypi.url}, Request Username/Password for the low index, SSL Context Service for a private CA). InvokeHTTP
          Response -> CryptographicHashContent (SHA-256) -> back to this processor. success -> the diode.""",
          keywords=["python", "pypi", "wheel", "diode", "low"])
@use_case(description="High side: upload each wheel from the diode to the high package index with native InvokeHTTP, "
                      "counting a file the index already holds as done.",
          configuration="""from the diode -> CryptographicHashContent (SHA-256) -> this processor (Transfer Side High,
          Upload URL = InvokeHTTP's HTTP URL). request -> InvokeHTTP (POST multipart, form name content, post:form:name
          ${pypi.name}, post:form:version ${pypi.version}, post:form:sha256_digest ${content_SHA-256},
          post:form:requires_python ${pypi.requires_python}, Response Body
          Attribute Name pypi.response). InvokeHTTP Original and No Retry -> back to this processor.""",
          keywords=["python", "pypi", "wheel", "diode", "high", "upload"])
class RoutePythonTransfer(FlowFileTransform):
    """Decides each step of a wheel around native InvokeHTTP and CryptographicHashContent. Never reads a wheel.

    pypi.stage says where a FlowFile is:
      Low,  none   a job from SplitJson (small JSON): request sends it to InvokeHTTP GET ${pypi.url}
      Low,  fetch  the downloaded wheel, after CryptographicHashContent: success labels it for the diode
      High, other  a wheel from the diode, after CryptographicHashContent: request sends it to InvokeHTTP POST
      High, upload after InvokeHTTP (Original or No Retry): success when uploaded or already in the index
    Anything wrong goes to failure with pypi.error.
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "0.6.1"
        description = (
            "Routes Python package files around native InvokeHTTP and CryptographicHashContent; it never reads one. "
            "pypi.stage says which step a FlowFile is at. "
            "LOW IN: a job from SplitJson on PlanPythonTransfer's $.jobs. OUT request: pypi.filename, pypi.name, "
            "pypi.version, pypi.sha256, pypi.size, pypi.url, pypi.requires_python, pypi.stage fetch, for InvokeHTTP GET "
            "${pypi.url}. "
            "IN again: InvokeHTTP Response after CryptographicHashContent (SHA-256). OUT success, once the bytes match "
            "pypi.sha256: filename, X-Sha256, X-Artifact-Format whl or sdist, X-Requires-Python, mime.type, pypi.stage sent. "
            "X-Artifact-Type, X-Artifact-Action and the X-CI-* audit headers are the webhook's, inherited through "
            "SplitJson, never set here. "
            "HIGH IN: a file from the diode after CryptographicHashContent, with filename, X-Sha256, X-Artifact-Format "
            "and an X-Artifact-Type starting python-packages. OUT request: pypi.name, pypi.version, pypi.filetype, "
            "pypi.pyversion (from the file name, never the low side), pypi.requires_python (X-Requires-Python when it is a "
            "PEP 440 specifier set, else empty), pypi.stage upload, pypi.upload.key. IN again: "
            "InvokeHTTP Original and No Retry. OUT success: pypi.result uploaded or duplicate. "
            "Any refusal goes to failure with pypi.error.")
        tags = ["python", "pypi", "wheel", "package", "route", "diode", "gitlab", "streaming"]

    SIDE = PropertyDescriptor(
        name="Transfer Side",
        description="Low (default): prepares downloads and labels verified wheels. High: checks wheels and reads upload answers.",
        required=True,
        default_value="Low",
        allowable_values=["Low", "High"])
    UPLOAD_URL = PropertyDescriptor(
        name="Upload URL",
        description=(
            "High side, required there: exactly the upload InvokeHTTP's HTTP URL, such as #{gitlab.pypi.url}. An answer "
            "from any other URL is refused, so attributes carried across the diode cannot pass as an upload. "
            "Environment variables allowed."),
        validators=[StandardValidators.URL_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.ENVIRONMENT)
    ALREADY_THERE = PropertyDescriptor(
        name="Already Uploaded Pattern",
        description=(
            "High side: a regular expression on \"<HTTP status> <response body>\" (InvokeHTTP Response Body Attribute Name "
            "pypi.response) meaning the index already holds the file, so it counts as done. The default covers GitLab, "
            "PyPI, pypiserver, Artifactory and Nexus."),
        required=True,
        # Literal values: NiFi reads descriptors by static parsing, which cannot follow an import.
        default_value="^(400|409) .*(already been taken|already exists|does not allow updating)",
        validators=[StandardValidators.REGULAR_EXPRESSION_VALIDATOR])

    ACCEPTED = PropertyDescriptor(
        name="Accepted Formats",
        description=(
            "Package files this side lets through: whl (default) wheels only, sdist source distributions "
            "(name-version.tar.gz or .zip) only, or whl,sdist both. Anything else is always refused. Pip builds an "
            "sdist on install, running its setup code."),
        required=True,
        default_value="whl",
        allowable_values=["whl", "sdist", "whl,sdist"])
    UPLOAD_SECRET = PropertyDescriptor(
        name="Upload Secret",
        description=(
            "High side. Proves an upload answer came from this processor, not across the diode. One sensitive "
            "parameter of 32 or more random characters, the same on every node: set it on a cluster or with Concurrent "
            "Tasks above 1. Empty: each Python process keys its own, so another one uploads again (duplicate)."),
        sensitive=True)

    PROPERTIES = [SIDE, UPLOAD_URL, ALREADY_THERE, ACCEPTED, UPLOAD_SECRET]

    def __init__(self, **kwargs):
        super().__init__()
        # Proves that an upload stage came from this processor, not across the diode, when Upload Secret is empty.
        # Each Python process has its own, and a restart forgets it: a wheel then in flight is checked and uploaded
        # again, and the index answers that it already holds the file.
        self.secret = os.urandom(32)

    def getRelationships(self):
        return [Relationship("request", "Low: a job ready for InvokeHTTP GET ${pypi.url}. High: a checked wheel ready for "
                                        "InvokeHTTP POST to Upload URL. Connect to that InvokeHTTP"),
                Relationship("success", "Low: a verified, labelled wheel; connect to the diode. High: uploaded, or already "
                                        "in the index (pypi.result uploaded or duplicate); auto-terminate"),
                Relationship("failure", "Refused: an allowlist or sha256 mismatch, a wheel that is not one, or an upload the "
                                        "index rejected. pypi.error says why; connect to a disabled hold processor")]

    def getPropertyDescriptors(self):
        return self.PROPERTIES

    def transform(self, context, flowfile):
        attributes = {name: flowfile.getAttribute(name) for name in READ}
        stage = flowfile.getAttribute(STAGE) or ""
        try:
            accepted = context.getProperty(self.ACCEPTED).getValue() or WHEEL_FORMAT
            if context.getProperty(self.SIDE).getValue() == "Low":
                return self._low(flowfile, stage, attributes, accepted)
            return self._high(context, stage, attributes, accepted)
        except TransferError as error:
            self.logger.error(f"Refused: {error}")
            return FlowFileTransformResult("failure", attributes={ERROR: str(error)})

    def _low(self, flowfile, stage, attributes, accepted):
        if stage == "fetch":
            refused = format_refused(attributes.get("pypi.filename"), accepted)
            if refused:
                raise TransferError(refused)
            labels = fetched_attributes(attributes)
            self.logger.info(f"Verified {labels['filename']}")
            return FlowFileTransformResult("success", attributes=labels)
        try:
            job = json.loads(flowfile.getContentsAsBytes())  # only a SplitJson job reaches here, never a wheel
        except ValueError:
            raise TransferError("not a job from PlanPythonTransfer") from None
        refused = not job.get("error") and format_refused(job.get("filename"), accepted)
        if refused:
            raise TransferError(refused)  # before download
        return FlowFileTransformResult("request", attributes=fetch_attributes(job))

    def _high(self, context, stage, attributes, accepted):
        shared = context.getProperty(self.UPLOAD_SECRET).getValue() or ""
        if shared and len(shared) < 32:
            raise TransferError("Upload Secret is shorter than 32 characters")
        secret = shared.encode() or self.secret
        key = upload_key(secret, attributes.get("uuid") or "", attributes.get("filename") or "")
        if stage == "upload" and hmac.compare_digest(attributes.get("pypi.upload.key") or "", key):
            upload_url = evaluate(context.getProperty(self.UPLOAD_URL), Attributes())
            if not upload_url:
                raise TransferError("Upload URL is not set")
            result = upload_result(attributes, upload_url, context.getProperty(self.ALREADY_THERE).getValue() or ALREADY_THERE)
            self.logger.info(f"{attributes['filename']}: {result}")
            return FlowFileTransformResult("success", attributes={"pypi.result": result})
        refused = format_refused(attributes.get("filename"), accepted)
        if refused:
            raise TransferError(refused)
        return FlowFileTransformResult("request", attributes=arrived_attributes(attributes, secret))
