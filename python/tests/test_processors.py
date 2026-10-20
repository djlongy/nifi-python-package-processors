"""PlanPythonTransfer and RoutePythonTransfer, driven the way the low and high flows drive them."""
import hashlib
import json
import unittest

import nifi_stub
from PlanPythonTransfer import PlanPythonTransfer
from RoutePythonTransfer import RoutePythonTransfer

LOW = "https://git.test/api/v4/projects/1/packages/pypi"
HIGH = "https://git.test/api/v4/projects/2/packages/pypi"
SIX = {"filename": "six-1.17.0-py2.py3-none-any.whl", "name": "six", "version": "1.17.0", "sha256": "a" * 64,
       "size": 11050, "url": f"{LOW}/files/{'a' * 64}/six-1.17.0-py2.py3-none-any.whl"}


class FlowFile:
    def __init__(self, body=b"", **attributes):
        self.body, self.attributes = body, attributes

    def getContentsAsBytes(self):
        return self.body

    def getAttribute(self, name):
        return self.attributes.get(name)


def request(files, **headers):
    body = json.dumps({"registry": LOW, "files": files}).encode()
    attributes = {"X-Artifact-Type": "python-packages-request", "X-Artifact-Format": "json",
                  "X-Sha256": hashlib.sha256(body).hexdigest()} | headers
    return FlowFile(body, **attributes)


def run(processor_type, flowfile, **values):
    processor = processor_type()
    processor.logger = nifi_stub.Logger()
    return processor.transform(nifi_stub.FakeContext(processor_type.PROPERTIES, values), flowfile)


def plan(flowfile, **values):
    return run(PlanPythonTransfer, flowfile, **({"Allowed URL Base": f"{LOW}/files/"} | values))


class PlanTest(unittest.TestCase):
    def test_one_job_per_wheel_with_every_field(self):
        result = plan(request([SIX, SIX | {"filename": "click-8.3.0-py3-none-any.whl"}]))
        self.assertEqual(result.relationship, "success")
        jobs = json.loads(result.contents)["jobs"]
        self.assertEqual(jobs[0], {key: str(value) for key, value in SIX.items()})
        self.assertEqual((result.attributes["pypi.job.count"], result.attributes["pypi.refused.count"]), ("2", "0"))

    def test_a_wheel_outside_the_allowlist_is_kept_with_its_error_and_the_rest_still_go(self):
        foreign = SIX | {"url": "https://evil.test/six.whl"}
        bad_name, bad_digest = SIX | {"filename": "six.tar.gz"}, SIX | {"sha256": "A" * 64}
        jobs = json.loads(plan(request([SIX, foreign, bad_name, bad_digest])).contents)["jobs"]
        self.assertNotIn("error", jobs[0])
        self.assertIn("is not under", jobs[1]["error"])
        self.assertIn("not a wheel file name", jobs[2]["error"])
        self.assertIn("64 lowercase hex", jobs[3]["error"])

    def test_the_whole_request_is_refused(self):
        cases = {"wrong type": request([SIX], **{"X-Artifact-Type": "python-packages"}),
                 "wrong format": request([SIX], **{"X-Artifact-Format": "whl"}),
                 "wrong digest": request([SIX], **{"X-Sha256": "0" * 64}),
                 "no digest": FlowFile(b"{}", **{"X-Artifact-Type": "python-packages-request", "X-Artifact-Format": "json"})}
        for name, flowfile in cases.items():
            with self.subTest(name):
                result = plan(flowfile)
                self.assertEqual(result.relationship, "failure")
                self.assertTrue(result.attributes["pypi.error"])

    def test_body_that_is_not_a_file_list_is_refused(self):
        for body in (b"not json", b"[]", b'{"files": {}}'):
            flowfile = FlowFile(body, **{"X-Artifact-Type": "python-packages-request", "X-Artifact-Format": "json",
                                         "X-Sha256": hashlib.sha256(body).hexdigest()})
            with self.subTest(body):
                self.assertEqual(plan(flowfile).relationship, "failure")

    def test_more_files_than_the_maximum_is_refused(self):
        result = plan(request([SIX] * 3), **{"Maximum Files": "2"})
        self.assertIn("more than Maximum Files (2)", result.attributes["pypi.error"])


def low(flowfile):
    return run(RoutePythonTransfer, flowfile, **{"Transfer Side": "Low"})


def high(flowfile):
    return run(RoutePythonTransfer, flowfile, **{"Transfer Side": "High", "Upload URL": HIGH})


class LowRouteTest(unittest.TestCase):
    def fetched(self, **overrides):
        job = low(FlowFile(json.dumps({k: str(v) for k, v in SIX.items()}).encode())).attributes
        return FlowFile(b"wheel", **(job | {"invokehttp.request.url": SIX["url"], "content_SHA-256": SIX["sha256"],
                                            "X-Sha256": "request body digest"} | overrides))

    def test_a_job_goes_to_invokehttp_with_its_url(self):
        result = low(FlowFile(json.dumps({k: str(v) for k, v in SIX.items()}).encode()))
        self.assertEqual(result.relationship, "request")
        self.assertEqual((result.attributes["pypi.url"], result.attributes["pypi.stage"]), (SIX["url"], "fetch"))

    def test_a_job_the_plan_refused_fails_with_its_reason(self):
        result = low(FlowFile(json.dumps({k: str(v) for k, v in SIX.items()} | {"error": "not under x"}).encode()))
        self.assertEqual((result.relationship, result.attributes["pypi.error"]), ("failure", "not under x"))

    def test_a_matching_wheel_is_labelled_for_the_diode(self):
        result = low(self.fetched())
        self.assertEqual(result.relationship, "success")
        self.assertEqual(result.attributes, {"filename": SIX["filename"], "X-Sha256": SIX["sha256"],
                                             "X-Artifact-Type": "python-packages", "X-Artifact-Format": "whl",
                                             "X-Artifact-Action": "mirror", "mime.type": "application/octet-stream",
                                             "pypi.stage": "sent"})

    def test_a_wheel_that_does_not_match_its_sha256_fails(self):
        result = low(self.fetched(**{"content_SHA-256": "b" * 64}))
        self.assertIn("does not match its sha256", result.attributes["pypi.error"])

    def test_a_wheel_fetched_from_another_url_fails(self):
        result = low(self.fetched(**{"invokehttp.request.url": "https://elsewhere.test/six.whl"}))
        self.assertIn("not fetched from its pypi.url", result.attributes["pypi.error"])

    def test_content_that_is_not_a_job_fails(self):
        self.assertEqual(low(FlowFile(b"\x00binary")).relationship, "failure")


class HighRouteTest(unittest.TestCase):
    def arrived(self, **overrides):
        return FlowFile(b"wheel", **({"filename": SIX["filename"], "X-Sha256": SIX["sha256"],
                                      "content_SHA-256": SIX["sha256"], "X-Artifact-Type": "python-packages",
                                      "X-Artifact-Format": "whl", "pypi.stage": "sent",
                                      "invokehttp.request.url": SIX["url"], "invokehttp.status.code": "200"} | overrides))

    def test_a_checked_wheel_goes_to_upload(self):
        result = high(self.arrived())
        self.assertEqual((result.relationship, result.attributes), ("request", {"pypi.stage": "upload"}))

    def test_a_wheel_that_is_not_one_fails(self):
        cases = {"type": {"X-Artifact-Type": "container-images"}, "format": {"X-Artifact-Format": "tar"},
                 "name": {"filename": "six.tar.gz"}, "digest": {"content_SHA-256": "b" * 64}}
        for name, overrides in cases.items():
            with self.subTest(name):
                self.assertEqual(high(self.arrived(**overrides)).relationship, "failure")

    def uploaded(self, status, response="", url=HIGH):
        return high(self.arrived(**{"pypi.stage": "upload", "invokehttp.request.url": url,
                                    "invokehttp.status.code": status, "pypi.response": response}))

    def test_upload_answers(self):
        self.assertEqual(self.uploaded("201").attributes, {"pypi.result": "uploaded"})
        duplicate = self.uploaded("400", '{"message":"Validation failed: File name has already been taken"}')
        self.assertEqual(duplicate.attributes, {"pypi.result": "duplicate"})
        self.assertIn("HTTP 403", self.uploaded("403", "forbidden").attributes["pypi.error"])
        self.assertEqual(self.uploaded("400", "bad request").relationship, "failure")
        for status, response in (("400", "File already exists. See https://pypi.org/help/#file-name-reuse"),
                                 ("409", "Conflict"), ("409", "six-1.17.0.whl already exists"),
                                 ("400", "Repository does not allow updating assets: pypi-hosted")):
            with self.subTest(status=status, response=response):
                expected = "failure" if response == "Conflict" else "success"
                self.assertEqual(self.uploaded(status, response).relationship, expected)

    def test_the_already_uploaded_pattern_is_a_property(self):
        result = run(RoutePythonTransfer, self.arrived(**{"pypi.stage": "upload", "invokehttp.request.url": HIGH,
                                                           "invokehttp.status.code": "409", "pypi.response": "Conflict"}),
                     **{"Transfer Side": "High", "Upload URL": HIGH, "Already Uploaded Pattern": "^409 Conflict$"})
        self.assertEqual(result.attributes, {"pypi.result": "duplicate"})

    def test_an_upload_stage_carried_across_the_diode_is_not_an_upload(self):
        result = self.uploaded("200", url=SIX["url"])
        self.assertIn("was not uploaded to", result.attributes["pypi.error"])


if __name__ == "__main__":
    unittest.main()
