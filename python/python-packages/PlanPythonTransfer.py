import json

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship

from python_packages import TransferError, plan_jobs

ERROR = "pypi.error"
HEADERS = ("X-Artifact-Type", "X-Artifact-Format", "X-Sha256")


class PlanPythonTransfer(FlowFileTransform):
    """Low side: turns one wheel request into a job list for SplitJson. Reads only the small JSON request."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "0.1.0"
        description = ("Checks a Python package request from the mirror's request job (X-Artifact-Type "
                       "python-packages-request, X-Artifact-Format json, X-Sha256 of the body) and emits "
                       "{\"jobs\": [...]}, one job per wheel, for SplitJson on $.jobs. A wheel whose url is not under "
                       "Allowed URL Base, or whose filename or sha256 is malformed, keeps an \"error\" in its job and "
                       "RoutePythonTransfer rejects it alone. Never downloads anything.")
        tags = ["python", "pypi", "wheel", "diode", "plan"]

    ALLOWED_BASE = PropertyDescriptor(
        name="Allowed URL Base",
        description="Every wheel url must start with this, such as #{low.pypi.url}/files/ (GitLab) or the index's file base URL. Never from the request.",
        required=True)
    MAX_FILES = PropertyDescriptor(
        name="Maximum Files",
        description="Most wheels one request may name; more refuses the request. The mirror sends 200 per request.",
        required=True, default_value="1000", validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR])

    PROPERTIES = [ALLOWED_BASE, MAX_FILES]

    def __init__(self, **kwargs):
        super().__init__()

    def getRelationships(self):
        return [Relationship("success", "One JSON job list, {\"jobs\": [...]}, for SplitJson on $.jobs"),
                Relationship("failure", "The request was refused; pypi.error says why. No wheel is fetched")]

    def getPropertyDescriptors(self):
        return self.PROPERTIES

    def transform(self, context, flowfile):
        attributes = {name: flowfile.getAttribute(name) for name in HEADERS}
        try:
            jobs = plan_jobs(flowfile.getContentsAsBytes(), attributes,
                             context.getProperty(self.ALLOWED_BASE).getValue(),
                             int(context.getProperty(self.MAX_FILES).getValue()))
        except TransferError as error:
            self.logger.error(f"Request refused: {error}")
            return FlowFileTransformResult("failure", attributes={ERROR: str(error)})
        refused = sum(1 for job in jobs if job.get("error"))
        self.logger.info(f"Planned {len(jobs)} wheel(s), {refused} refused by the allowlist")
        return FlowFileTransformResult("success", contents=json.dumps({"jobs": jobs}).encode(),
                                       attributes={"pypi.job.count": str(len(jobs)), "pypi.refused.count": str(refused),
                                                   "mime.type": "application/json"})
