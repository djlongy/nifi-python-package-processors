import json

from nifiapi.documentation import use_case
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import ExpressionLanguageScope, PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship

from nifi_attributes import Attributes, evaluate
from python_packages import (TransferError, allowed_bases, index_lookup, plan_jobs, trust_context, trusted_der,
                             web_opener)

ERROR = "pypi.error"
HEADERS = ("X-Artifact-Type", "X-Artifact-Format", "X-Sha256")


@use_case(description="Turn the request a gitlab-pypi-mirror pipeline POSTs into one job per wheel, for native InvokeHTTP "
                      "to fetch from the low package index.",
          configuration="""ListenHTTP (HTTP Headers for Attributes (?i)x-.*), then this processor with Allowed URL Base,
          then SplitJson on $.jobs, then RoutePythonTransfer (Transfer Side Low). The request is the JSON
          {"registry", "files": [{filename, name, version, sha256, size, url}]} with X-Artifact-Type
          python-packages-request, X-Artifact-Format json and X-Sha256 of the body.""",
          keywords=["python", "pypi", "wheel", "diode", "webhook"])
@use_case(description="Resend named wheels from NiFi, without the pipeline.",
          configuration="""GenerateFlowFile (kept stopped, Run Once) with Custom Text holding wheel file names, one per
          line, and dynamic properties X-Artifact-Type python-packages-request and X-Artifact-Format text, connected to
          this processor. Set Index URL (and Index Username, Index Password, SSL Context Service when needed): each name
          is looked up in the index's PEP 503 /simple/<project>/ page for its url and sha256.""",
          keywords=["python", "pypi", "wheel", "resend"])
class PlanPythonTransfer(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "0.4.2"
        description = ("Plans the transfer of Python wheels: checks a request (X-Artifact-Type python-packages-request, "
                       "X-Artifact-Format json with X-Sha256 of the body, or text: wheel file names looked up in Index URL) "
                       "and emits one JSON job list, {\"jobs\": [...]}, for SplitJson on $.jobs. Each job names a wheel's "
                       "filename, name, version, sha256, size and url. It never reads a wheel: native InvokeHTTP downloads "
                       "each one. A wheel whose url is not under Allowed URL Base, or whose filename or sha256 is malformed, "
                       "keeps an error in its job, and RoutePythonTransfer refuses that wheel alone.")
        tags = ["python", "pypi", "wheel", "package", "plan", "diode", "gitlab", "webhook"]

    ALLOWED_BASE = PropertyDescriptor(
        name="Allowed URL Base",
        description=(
            "Every wheel url must start with one of these (whitespace or comma separated), such as "
            "#{low.pypi.url}/files/. A group index links to its projects: give https://gitlab.example.com/api/v4/projects/ "
            "or each project's files URL. Never taken from the request. Environment variables allowed."),
        required=True,
        expression_language_scope=ExpressionLanguageScope.ENVIRONMENT)
    MAX_FILES = PropertyDescriptor(
        name="Maximum Files",
        description="Most wheels one request may name; more refuses the whole request. Default 1000. The mirror sends 200.",
        required=True,
        default_value="1000",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR])
    INDEX_URL = PropertyDescriptor(
        name="Index URL",
        description=(
            "Text requests only: the low index, such as https://gitlab.example.com/api/v4/projects/12/packages/pypi, whose "
            "/simple/<project>/ pages give each wheel's url and sha256. No user:password in the URL. Empty (default): text "
            "requests are refused. Environment variables allowed."),
        validators=[StandardValidators.URL_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.ENVIRONMENT)
    INDEX_USERNAME = PropertyDescriptor(
        name="Index Username",
        description=("User for Index URL, such as __token__ for a GitLab token. Empty (default): anonymous. "
                     "Environment variables allowed."),
        expression_language_scope=ExpressionLanguageScope.ENVIRONMENT)
    INDEX_PASSWORD = PropertyDescriptor(
        name="Index Password",
        description="Password or token for Index URL. Empty (default): none sent. Sensitive: reference a parameter, such as #{low.pypi.token}.",
        sensitive=True)
    SSL_CONTEXT = PropertyDescriptor(
        name="SSL Context Service",
        description=(
            "A StandardSSLContextService whose PKCS12 or JKS truststore holds the index's CA (it does not accept PEM). Its "
            "CAs add to the system store and SSL_CERT_FILE. Empty (default): only those are trusted. Its keystore is ignored."),
        controller_service_definition="org.apache.nifi.ssl.SSLContextProvider")
    TIMEOUT = PropertyDescriptor(
        name="Request Timeout",
        description="Seconds an index page read may stall before the request fails. Default 60. Wheels are never read here.",
        required=True,
        default_value="60",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR])

    PROPERTIES = [ALLOWED_BASE, MAX_FILES, INDEX_URL, INDEX_USERNAME, INDEX_PASSWORD, SSL_CONTEXT, TIMEOUT]

    def __init__(self, **kwargs):
        super().__init__()

    def getRelationships(self):
        return [Relationship("success", "One JSON job list, {\"jobs\": [...]}, mime.type application/json, with "
                                        "pypi.job.count and pypi.refused.count. Connect to SplitJson on $.jobs"),
                Relationship("failure", "The whole request was refused (headers, X-Sha256, body, size or index); "
                                        "pypi.error says why and no wheel is fetched. Connect to a disabled hold processor")]

    def getPropertyDescriptors(self):
        return self.PROPERTIES

    def _lookup(self, context):
        """The index lookup for text requests, or None when Index URL is empty."""
        environment = Attributes()
        index = evaluate(context.getProperty(self.INDEX_URL), environment)
        if not index:
            return None
        opener = web_opener(trust_context(trusted_der(context.getProperty(self.SSL_CONTEXT))))
        return index_lookup(index, evaluate(context.getProperty(self.INDEX_USERNAME), environment) or "",
                            context.getProperty(self.INDEX_PASSWORD).getValue() or "",
                            int(context.getProperty(self.TIMEOUT).getValue()), opener,
                            allowed_bases(evaluate(context.getProperty(self.ALLOWED_BASE), Attributes())))

    def transform(self, context, flowfile):
        attributes = {name: flowfile.getAttribute(name) for name in HEADERS}
        try:
            jobs = plan_jobs(flowfile.getContentsAsBytes(), attributes,
                             evaluate(context.getProperty(self.ALLOWED_BASE), Attributes()),
                             int(context.getProperty(self.MAX_FILES).getValue()), self._lookup(context))
        except TransferError as error:
            self.logger.error(f"Request refused: {error}")
            return FlowFileTransformResult("failure", attributes={ERROR: str(error)})
        refused = sum(1 for job in jobs if job.get("error"))
        self.logger.info(f"Planned {len(jobs)} wheel(s), {refused} refused by the allowlist")
        return FlowFileTransformResult("success", contents=json.dumps({"jobs": jobs}).encode(),
                                       attributes={"pypi.job.count": str(len(jobs)), "pypi.refused.count": str(refused),
                                                   "mime.type": "application/json"})
