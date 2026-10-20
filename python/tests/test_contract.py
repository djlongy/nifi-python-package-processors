"""The request gitlab-pypi-mirror sends, captured from its post_request into fixtures/gitlab-pypi-mirror-request.json,
goes through both processors the way ListenHTTP hands it over: every X- header as an attribute, Filename as filename.
gitlab-pypi-mirror's tests check its side against the same header list. Recapture after changing either side."""
import json
import os
import unittest

import nifi_stub  # noqa: F401
from test_processors import FlowFile, run
from PlanPythonTransfer import PlanPythonTransfer
from RoutePythonTransfer import RoutePythonTransfer

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "gitlab-pypi-mirror-request.json")
HEADERS = ["Content-Type", "Filename", "X-Artifact-Action", "X-Artifact-Format", "X-Artifact-Type", "X-CI-Commit-Sha",
           "X-CI-Pipeline-Url", "X-CI-Project", "X-File-Count", "X-Registry", "X-Sha256"]


class MirrorContractTest(unittest.TestCase):
    def setUp(self):
        with open(FIXTURE) as handle:
            captured = json.load(handle)
        self.headers, self.body = captured["headers"], captured["body"].encode()
        self.attributes = {k: v for k, v in self.headers.items() if k.startswith("X-")} | {"filename": self.headers["Filename"]}
        self.base = json.loads(self.body)["registry"] + "/files/"

    def test_the_mirror_sends_the_headers_the_flow_routes_on(self):
        self.assertEqual(sorted(self.headers), HEADERS)
        self.assertEqual(self.headers["X-Artifact-Type"], "python-packages-request")

    def test_each_wheel_crosses_with_the_request_attributes_and_python_packages(self):
        plan = run(PlanPythonTransfer, FlowFile(self.body, **self.attributes), **{"Allowed URL Base": self.base})
        self.assertEqual(plan.relationship, "success", plan.attributes)
        jobs = json.loads(plan.contents)["jobs"]
        self.assertEqual(len(jobs), int(self.headers["X-File-Count"]))
        for job in jobs:
            with self.subTest(job["filename"]):
                self.assertNotIn("error", job)
                route = run(RoutePythonTransfer, FlowFile(json.dumps(job).encode(), **self.attributes), **{"Transfer Side": "Low"})
                self.assertEqual(route.relationship, "request")
                fetched = self.attributes | route.attributes | {"content_SHA-256": job["sha256"],
                                                                "invokehttp.request.url": job["url"]}
                labelled = run(RoutePythonTransfer, FlowFile(b"wheel", **fetched), **{"Transfer Side": "Low"})
                self.assertEqual(labelled.relationship, "success", labelled.attributes)
                out = fetched | labelled.attributes
                self.assertEqual((out["X-Artifact-Type"], out["filename"]), ("python-packages", job["filename"]))
                for kept in ("X-Registry", "X-CI-Project", "X-CI-Pipeline-Url", "X-CI-Commit-Sha"):
                    self.assertEqual(out[kept], self.headers[kept])


if __name__ == "__main__":
    unittest.main()
