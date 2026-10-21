"""PlanPythonTransfer and RoutePythonTransfer, driven the way the low and high flows drive them."""
import hashlib
import json
import unittest

import io

import nifi_stub
import python_packages
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


def run(processor, flowfile, **values):
    """processor is a class, for a fresh instance, or an instance that keeps its state between FlowFiles."""
    processor = processor() if isinstance(processor, type) else processor
    processor.logger = nifi_stub.Logger()
    return processor.transform(nifi_stub.FakeContext(processor.PROPERTIES, values), flowfile)


def plan(flowfile, **values):
    return run(PlanPythonTransfer, flowfile, **({"Allowed URL Base": f"{LOW}/files/"} | values))


class PlanTest(unittest.TestCase):
    def test_one_job_per_wheel_with_every_field(self):
        click = "click-8.3.0-py3-none-any.whl"
        result = plan(request([SIX, SIX | {"filename": click, "url": f"{LOW}/files/{'a' * 64}/{click}"}]))
        self.assertEqual(result.relationship, "success")
        jobs = json.loads(result.contents)["jobs"]
        self.assertEqual(jobs[0], {key: str(value) for key, value in SIX.items()})
        self.assertEqual((result.attributes["pypi.job.count"], result.attributes["pypi.refused.count"]), ("2", "0"))

    def test_a_url_that_only_starts_with_the_base_is_refused(self):
        base = f"{LOW}/files/"
        for url in (f"{base}../../../3/packages/x.whl", f"{base}%2e%2e/x.whl", f"{base}a/./x.whl",
                    f"{base}a%2Fb.whl", f"{base}a\\..\\x.whl", f"{base}x.whl?u=1", f"{base}x.whl#f",
                    f"{base}%2E%2E/x.whl"):
            with self.subTest(url):
                self.assertTrue(python_packages.url_outside(url, base))
        self.assertFalse(python_packages.url_outside(SIX["url"], base))

    def test_several_allowed_bases_and_a_group_of_projects(self):
        bases = f"https://other.example/files/, {LOW}/files/"
        self.assertFalse(python_packages.url_outside(SIX["url"], bases))
        self.assertFalse(python_packages.url_outside("https://other.example/files/x.whl", bases))
        self.assertTrue(python_packages.url_outside("https://evil.example/files/x.whl", bases))
        projects = "https://git.test/api/v4/projects/"
        self.assertFalse(python_packages.url_outside(SIX["url"], projects))
        self.assertTrue(python_packages.url_outside("https://git.test/api/v4/groups/1/x.whl", projects))
        self.assertIn("is not under https://other.example/files/ or", python_packages.entry_error(
            {k: str(v) for k, v in SIX.items()} | {"url": "https://evil.example/x.whl"}, bases))

    def test_a_wheel_outside_the_allowlist_is_kept_with_its_error_and_the_rest_still_go(self):
        foreign = SIX | {"url": "https://evil.test/six.whl"}
        bad_name, bad_digest = SIX | {"filename": "six.tar.gz"}, SIX | {"sha256": "A" * 64}
        jobs = json.loads(plan(request([SIX, foreign, bad_name, bad_digest])).contents)["jobs"]
        self.assertNotIn("error", jobs[0])
        self.assertIn("is not under", jobs[1]["error"])
        self.assertIn("not a wheel or source distribution file name", jobs[2]["error"])
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


SIMPLE = (f'<html><body><a href="{LOW}/files/{"a" * 64}/six-1.17.0-py2.py3-none-any.whl#sha256={"a" * 64}">'
          'six-1.17.0-py2.py3-none-any.whl</a></body></html>')


class FakeIndex:
    """An opener answering /simple/six/ with SIMPLE and recording what was asked and with which credentials."""
    def __init__(self):
        self.asked = []

    def open(self, request, timeout=None):
        self.asked.append((request.full_url, request.get_header("Authorization")))
        if not request.full_url.endswith("/simple/six/"):
            raise OSError("HTTP Error 404: Not Found")
        return io.BytesIO(SIMPLE.encode())


def text_request(text, **headers):
    return FlowFile(text.encode(), **({"X-Artifact-Type": "python-packages-request", "X-Artifact-Format": "text"} | headers))


class TextRequestTest(unittest.TestCase):
    """A resend typed into GenerateFlowFile: wheel file names, looked up in the low index."""

    def plan(self, flowfile, index=None):
        index = index or FakeIndex()
        lookup = python_packages.index_lookup(LOW, "__token__", "secret", opener=index)
        return python_packages.plan_jobs(flowfile.getContentsAsBytes(), flowfile.attributes, f"{LOW}/files/", 1000,
                                         lookup), index

    def test_each_named_wheel_gets_its_url_and_sha256_from_the_index(self):
        jobs, index = self.plan(text_request("# resend\nsix-1.17.0-py2.py3-none-any.whl\n\n"))
        self.assertEqual(jobs, [{"filename": SIX["filename"], "name": "six", "version": "1.17.0", "sha256": "a" * 64,
                                 "size": "", "url": SIX["url"]}])
        self.assertEqual(index.asked, [(f"{LOW}/simple/six/", "Basic X190b2tlbl9fOnNlY3JldA==")])

    def test_a_wheel_the_index_does_not_list_is_refused_alone(self):
        jobs, _ = self.plan(text_request("six-1.17.0-py2.py3-none-any.whl\nsix-9.9-py3-none-any.whl\n"))
        self.assertNotIn("error", jobs[0])
        self.assertIn("is not in the index", jobs[1]["error"])

    def test_an_unreachable_index_refuses_the_request(self):
        with self.assertRaisesRegex(python_packages.TransferError, "404"):
            self.plan(text_request("idna-3.20-py3-none-any.whl\n"))

    def test_a_given_x_sha256_is_checked_and_an_empty_list_is_refused(self):
        with self.assertRaisesRegex(python_packages.TransferError, "X-Sha256"):
            self.plan(text_request("six-1.17.0-py2.py3-none-any.whl", **{"X-Sha256": "0" * 64}))
        with self.assertRaisesRegex(python_packages.TransferError, "names no wheel"):
            self.plan(text_request("# nothing\n"))

    def test_the_processor_refuses_a_text_request_without_an_index(self):
        result = plan(text_request("six-1.17.0-py2.py3-none-any.whl"))
        self.assertIn("needs Index URL", result.attributes["pypi.error"])


def low(flowfile, **values):
    return run(RoutePythonTransfer, flowfile, **({"Transfer Side": "Low"} | values))


HIGH_ROUTE = RoutePythonTransfer()


def high(flowfile, **values):
    return run(HIGH_ROUTE, flowfile, **({"Transfer Side": "High", "Upload URL": HIGH} | values))


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
                                      "X-Artifact-Format": "whl", "pypi.stage": "sent", "uuid": "u-1",
                                      "pypi.name": "evil", "pypi.version": "6.6.6", "pypi.response": "low answer",
                                      "invokehttp.request.url": SIX["url"], "invokehttp.status.code": "200"} | overrides))

    def test_a_checked_wheel_goes_to_upload_named_by_its_file_and_with_no_earlier_answer(self):
        result = high(self.arrived())
        self.assertEqual(result.relationship, "request")
        self.assertEqual({k: v for k, v in result.attributes.items() if k != "pypi.upload.key"},
                         {"pypi.stage": "upload", "pypi.name": "six", "pypi.version": "1.17.0", "pypi.response": "",
                          "pypi.filetype": "bdist_wheel", "pypi.pyversion": "py2.py3", "invokehttp.status.code": "",
                          "invokehttp.request.url": "", "invokehttp.response.url": ""})
        self.assertEqual(len(result.attributes["pypi.upload.key"]), 64)

    def test_a_wheel_that_is_not_one_fails(self):
        cases = {"type": {"X-Artifact-Type": "container-images"}, "format": {"X-Artifact-Format": "tar"},
                 "name": {"filename": "six.tar.gz"}, "digest": {"content_SHA-256": "b" * 64}}
        for name, overrides in cases.items():
            with self.subTest(name):
                self.assertEqual(high(self.arrived(**overrides)).relationship, "failure")

    def uploaded(self, status, response="", url=HIGH, **values):
        """A wheel through the high route, then InvokeHTTP's answer on the same FlowFile, back into the same route."""
        flowfile = self.arrived()
        flowfile.attributes |= high(flowfile, **values).attributes
        flowfile.attributes |= {"invokehttp.request.url": url, "invokehttp.status.code": status, "pypi.response": response}
        return high(flowfile, **values)

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
        result = self.uploaded("409", "Conflict", **{"Already Uploaded Pattern": "^409 Conflict$"})
        self.assertEqual(result.attributes, {"pypi.result": "duplicate"})

    def test_an_answer_from_another_url_is_not_an_upload(self):
        result = self.uploaded("200", url=SIX["url"])
        self.assertIn("was not uploaded to", result.attributes["pypi.error"])

    def test_an_upload_redirected_elsewhere_is_not_an_upload(self):
        flowfile = self.arrived()
        flowfile.attributes |= high(flowfile).attributes
        flowfile.attributes |= {"invokehttp.request.url": HIGH, "invokehttp.status.code": "200",
                                "invokehttp.response.url": "https://login.test/sign_in"}
        self.assertIn("was not uploaded to", high(flowfile).attributes["pypi.error"])

    def test_an_upload_stage_carried_across_the_diode_is_checked_and_uploaded_again(self):
        forged = {"pypi.stage": "upload", "invokehttp.request.url": HIGH, "invokehttp.status.code": "201"}
        for key in ({}, {"pypi.upload.key": "0" * 64}, {"pypi.upload.key": high(self.arrived()).attributes["pypi.upload.key"],
                                                        "uuid": "u-2"}):
            with self.subTest(key=key):
                result = high(self.arrived(**forged | key))
                self.assertEqual((result.relationship, result.attributes["pypi.stage"]), ("request", "upload"))

    def test_a_shared_upload_secret_lets_any_instance_read_the_answer(self):
        shared = {"Upload Secret": "s" * 32}
        stamped = self.arrived()
        stamped.attributes |= high(stamped, **shared).attributes | {"invokehttp.request.url": HIGH,
                                                                     "invokehttp.status.code": "201"}
        result = run(RoutePythonTransfer, stamped, **{"Transfer Side": "High", "Upload URL": HIGH} | shared)
        self.assertEqual(result.attributes, {"pypi.result": "uploaded"})

    def test_a_short_upload_secret_is_refused(self):
        result = high(self.arrived(), **{"Upload Secret": "short"})
        self.assertIn("shorter than 32", result.attributes["pypi.error"])

    def test_another_route_instance_does_not_accept_the_key(self):
        stamped = self.arrived()
        stamped.attributes |= high(stamped).attributes | {"invokehttp.request.url": HIGH, "invokehttp.status.code": "201"}
        result = run(RoutePythonTransfer, stamped, **{"Transfer Side": "High", "Upload URL": HIGH})
        self.assertEqual(result.relationship, "request")


PYCRC = {"filename": "pycrc16-0.1.2.tar.gz", "name": "pycrc16", "version": "0.1.2", "sha256": "c" * 64, "size": 112640,
         "url": f"{LOW}/files/{'c' * 64}/pycrc16-0.1.2.tar.gz"}


BOTH = {"Accepted Formats": "whl,sdist"}


class SdistTest(unittest.TestCase):
    """A source distribution the mirror sends with MIRROR_PUBLISH_SDISTS crosses like a wheel, labelled sdist, where
    Accepted Formats lets it through."""

    def test_by_default_an_sdist_is_refused_before_download_and_on_arrival(self):
        result = low(FlowFile(json.dumps({k: str(v) for k, v in PYCRC.items()}).encode()))
        self.assertEqual(result.relationship, "failure")
        self.assertIn("Accepted Formats (whl) refuses", result.attributes["pypi.error"])
        arrived = {"filename": PYCRC["filename"], "X-Sha256": PYCRC["sha256"], "content_SHA-256": PYCRC["sha256"],
                   "X-Artifact-Type": "python-packages", "X-Artifact-Format": "sdist", "uuid": "u-9"}
        self.assertEqual(high(FlowFile(b"sdist", **arrived)).relationship, "failure")

    def test_sdist_alone_refuses_a_wheel(self):
        result = low(FlowFile(json.dumps({k: str(v) for k, v in SIX.items()}).encode()), **{"Accepted Formats": "sdist"})
        self.assertIn("is a whl", result.attributes["pypi.error"])

    def test_an_sdist_is_planned_fetched_and_labelled_sdist(self):
        jobs = json.loads(plan(request([PYCRC, PYCRC | {"filename": "pyreadline-2.1.zip", "url": f"{LOW}/files/{'c' * 64}/pyreadline-2.1.zip"}])).contents)["jobs"]
        self.assertFalse([job for job in jobs if "error" in job])
        fetched = {f"pypi.{k}": str(v) for k, v in PYCRC.items()} | {
            "pypi.stage": "fetch", "content_SHA-256": PYCRC["sha256"], "invokehttp.request.url": PYCRC["url"]}
        result = low(FlowFile(b"sdist", **fetched), **BOTH)
        self.assertEqual((result.relationship, result.attributes["X-Artifact-Format"], result.attributes["filename"]),
                         ("success", "sdist", PYCRC["filename"]))

    def test_the_high_side_uploads_an_sdist_named_by_its_file(self):
        arrived = {"filename": PYCRC["filename"], "X-Sha256": PYCRC["sha256"], "content_SHA-256": PYCRC["sha256"],
                   "X-Artifact-Type": "python-packages", "X-Artifact-Format": "sdist", "uuid": "u-9"}
        result = high(FlowFile(b"sdist", **arrived), **BOTH)
        self.assertEqual(result.relationship, "request")
        self.assertEqual((result.attributes["pypi.name"], result.attributes["pypi.version"],
                          result.attributes["pypi.filetype"], result.attributes["pypi.pyversion"]),
                         ("pycrc16", "0.1.2", "sdist", "source"))

    def test_a_label_that_does_not_match_the_file_is_refused(self):
        for filename, kind in ((PYCRC["filename"], "whl"), (SIX["filename"], "sdist"), ("pycrc16-0.1.2.tar.bz2", "sdist")):
            with self.subTest(filename=filename, kind=kind):
                arrived = {"filename": filename, "X-Sha256": "c" * 64, "content_SHA-256": "c" * 64,
                           "X-Artifact-Type": "python-packages", "X-Artifact-Format": kind}
                self.assertEqual(high(FlowFile(b"x", **arrived), **BOTH).relationship, "failure")


class ReviewFixTest(unittest.TestCase):
    def test_a_job_whose_url_names_another_file_is_refused(self):
        evil = SIX | {"url": f"{LOW}/files/{'a' * 64}/evil-9.9.9-py3-none-any.whl"}
        jobs = json.loads(plan(request([evil])).contents)["jobs"]
        self.assertIn("is not the file its url names", jobs[0]["error"])

    def test_too_many_names_are_refused_before_any_lookup(self):
        def lookup(name):
            raise AssertionError("looked up")
        body = "\n".join(f"p{i}-1.0-py3-none-any.whl" for i in range(3)).encode()
        with self.assertRaisesRegex(python_packages.TransferError, "more than Maximum Files"):
            python_packages.plan_jobs(body, {"X-Artifact-Type": "python-packages-request", "X-Artifact-Format": "text"},
                                      f"{LOW}/files/", 2, lookup)

    def test_a_name_that_is_not_a_pep_427_wheel_is_refused_on_arrival(self):
        for filename in ("a.whl", "requests-2.31.0-py3-none-any-extra.whl", "foo-1.0-1-extra-py3-none-any.whl"):
            with self.subTest(filename=filename):
                arrived = {"filename": filename, "X-Sha256": "a" * 64, "content_SHA-256": "a" * 64,
                           "X-Artifact-Type": "python-packages", "X-Artifact-Format": "whl", "uuid": "u"}
                self.assertEqual(high(FlowFile(b"x", **arrived)).relationship, "failure")
        self.assertEqual(python_packages.wheel_name_version("foo-1.0-1-py3-none-any.whl"), ("foo", "1.0"))

    def test_descriptor_literals_match_the_rules(self):
        props = {d.name: d for d in RoutePythonTransfer.PROPERTIES}
        self.assertEqual(props["Already Uploaded Pattern"].default_value, python_packages.ALREADY_THERE)
        self.assertEqual(props["Accepted Formats"].allowable_values, list(python_packages.ACCEPTED_FORMATS))


if __name__ == "__main__":
    unittest.main()
