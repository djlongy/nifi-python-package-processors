import json

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor
from nifiapi.relationship import Relationship

from python_packages import (ALREADY_THERE, TransferError, arrived_attributes, fetch_attributes, fetched_attributes,
                             upload_result)

ERROR, STAGE = "pypi.error", "pypi.stage"
READ = ("pypi.filename", "pypi.url", "pypi.sha256", "filename", "X-Sha256", "X-Artifact-Type", "X-Artifact-Format",
        "content_SHA-256", "invokehttp.request.url", "invokehttp.status.code", "pypi.response")


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
        version = "0.1.0"
        description = ("Routes Python wheels around native InvokeHTTP: on the low side it prepares each job from "
                       "PlanPythonTransfer for download and labels the wheel once its sha256 matches; on the high side "
                       "it checks the wheel from the diode before upload and reads the index's answer after it (a file "
                       "the index already holds counts as done). The pypi.stage attribute says which step a FlowFile is at. Never reads "
                       "wheel content.")
        tags = ["python", "pypi", "wheel", "diode", "route"]

    SIDE = PropertyDescriptor(name="Transfer Side", description="Low fetches from the low registry; High uploads to the high one.",
                              required=True, default_value="Low", allowable_values=["Low", "High"])
    UPLOAD_URL = PropertyDescriptor(
        name="Upload URL",
        description="High side: the exact HTTP URL of the upload InvokeHTTP, such as #{high.pypi.url}. An answer "
                    "from any other URL is refused, so an attribute carried across the diode cannot pass as an upload.")

    ALREADY_THERE = PropertyDescriptor(
        name="Already Uploaded Pattern",
        description="High side: a regular expression on \"<HTTP status> <response body>\" (InvokeHTTP Response Body "
                    "Attribute Name pypi.response) that means the index already holds the file, so the wheel counts "
                    "as done. The default covers GitLab, PyPI, pypiserver, Artifactory and Nexus.",
        default_value=ALREADY_THERE)

    PROPERTIES = [SIDE, UPLOAD_URL, ALREADY_THERE]

    def __init__(self, **kwargs):
        super().__init__()

    def getRelationships(self):
        return [Relationship("request", "Low: to InvokeHTTP GET. High: to InvokeHTTP POST"),
                Relationship("success", "Low: a verified, labelled wheel for the diode. High: uploaded, or already in the index"),
                Relationship("failure", "Refused; pypi.error says why")]

    def getPropertyDescriptors(self):
        return self.PROPERTIES

    def transform(self, context, flowfile):
        attributes = {name: flowfile.getAttribute(name) for name in READ}
        stage = flowfile.getAttribute(STAGE) or ""
        try:
            if context.getProperty(self.SIDE).getValue() == "Low":
                return self._low(flowfile, stage, attributes)
            return self._high(context, stage, attributes)
        except TransferError as error:
            self.logger.error(f"Refused: {error}")
            return FlowFileTransformResult("failure", attributes={ERROR: str(error)})

    def _low(self, flowfile, stage, attributes):
        if stage == "fetch":
            labels = fetched_attributes(attributes)
            self.logger.info(f"Verified {labels['filename']}")
            return FlowFileTransformResult("success", attributes=labels)
        try:
            job = json.loads(flowfile.getContentsAsBytes())  # only a SplitJson job reaches here, never a wheel
        except ValueError:
            raise TransferError("not a job from PlanPythonTransfer") from None
        return FlowFileTransformResult("request", attributes=fetch_attributes(job))

    def _high(self, context, stage, attributes):
        if stage == "upload":
            result = upload_result(attributes, context.getProperty(self.UPLOAD_URL).getValue() or "",
                                   context.getProperty(self.ALREADY_THERE).getValue() or ALREADY_THERE)
            self.logger.info(f"{attributes['filename']}: {result}")
            return FlowFileTransformResult("success", attributes={"pypi.result": result})
        return FlowFileTransformResult("request", attributes=arrived_attributes(attributes))
