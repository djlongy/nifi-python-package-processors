"""SSL Context Service trust, index URL rules and redirects, for PlanPythonTransfer's index lookups."""
import http.server
import ssl
import threading
import types
import unittest

import nifi_stub  # noqa: F401
from python_packages import (TransferError, check_index_url, index_lookup, same_origin, trust_context, trusted_der,
                             web_opener)


class Issuer:
    def __init__(self, der):
        self.der = der

    def getEncoded(self):
        return self.der


def service_property(value, issuers):
    """An SSL Context Service property: issuers None means a service with no truststore."""
    manager = types.SimpleNamespace(getAcceptedIssuers=lambda: [Issuer(d) for d in issuers or []])
    service = types.SimpleNamespace(createTrustManager=lambda: manager if issuers is not None else None)
    return types.SimpleNamespace(getValue=lambda: value, asControllerService=lambda: service)


class TrustTest(unittest.TestCase):
    def test_no_service_trusts_only_the_system_store(self):
        self.assertEqual(trusted_der(service_property(None, [])), [])

    def test_the_service_cas_are_handed_over_as_der(self):
        self.assertEqual(trusted_der(service_property("ssl-id", [b"\x30\x01", b"\x30\x02"])), [b"\x30\x01", b"\x30\x02"])

    def test_a_service_without_a_truststore_is_refused(self):
        with self.assertRaisesRegex(TransferError, "no truststore"):
            trusted_der(service_property("ssl-id", None))

    def test_verification_is_never_turned_off(self):
        context = trust_context()
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)


class IndexUrlTest(unittest.TestCase):
    def test_credentials_in_the_url_and_other_schemes_are_refused(self):
        for url in ("https://u:p@gitlab.example.com/pypi", "file:///etc/passwd", "ftp://example.com/x", ""):
            with self.subTest(url), self.assertRaises(TransferError):
                check_index_url(url)
        self.assertEqual(check_index_url("https://gitlab.example.com/pypi/"), "https://gitlab.example.com/pypi")
        with self.assertRaises(TransferError) as caught:
            check_index_url("ftp://user:canary-1234@gitlab.example.com/pypi")
        self.assertNotIn("canary-1234", str(caught.exception))


class OriginTest(unittest.TestCase):
    def test_a_link_under_the_index_path_is_read_from_the_index_host(self):
        index = "https://git.example.com/api/v4/projects/1/packages/pypi"
        link = "https://git.example.internal/api/v4/projects/1/packages/pypi/files/ab/six-1.17.0-py2.py3-none-any.whl"
        self.assertEqual(same_origin(link, index), index + "/files/ab/six-1.17.0-py2.py3-none-any.whl")
        for elsewhere in ("https://evil.example/x.whl", "https://git.example.internal/api/v4/projects/2/packages/pypi/x.whl"):
            self.assertEqual(same_origin(elsewhere, index), elsewhere)


class Redirecting(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(302)
        self.send_header("Location", "http://elsewhere.example/steal")
        self.end_headers()

    def log_message(self, *args):
        pass


class RedirectTest(unittest.TestCase):
    def test_a_redirect_is_refused_so_credentials_never_follow_it(self):
        server = http.server.HTTPServer(("127.0.0.1", 0), Redirecting)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        lookup = index_lookup(f"http://127.0.0.1:{server.server_port}/pypi", "__token__", "secret",
                              opener=web_opener(trust_context()))
        with self.assertRaisesRegex(TransferError, "redirect"):
            lookup("six-1.17.0-py2.py3-none-any.whl")


if __name__ == "__main__":
    unittest.main()
