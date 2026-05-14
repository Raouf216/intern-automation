import importlib.util
import sys
import types
import unittest
from pathlib import Path


class FakePlaywrightTimeoutError(Exception):
    pass


class FakeFastAPI:
    def __init__(self, *args, **kwargs):
        pass

    def add_middleware(self, *args, **kwargs):
        pass

    def get(self, *args, **kwargs):
        return lambda func: func

    def post(self, *args, **kwargs):
        return lambda func: func

    def api_route(self, *args, **kwargs):
        return lambda func: func


class FakeJSONResponse(dict):
    def __init__(self, status_code=200, content=None):
        super().__init__(content or {})
        self.status_code = status_code
        self.content = content or {}


def install_import_stubs():
    sys.modules.setdefault("httpx", types.SimpleNamespace())
    sys.modules.setdefault("fastapi", types.SimpleNamespace(FastAPI=FakeFastAPI))
    sys.modules.setdefault("fastapi.middleware", types.SimpleNamespace())
    sys.modules.setdefault("fastapi.middleware.cors", types.SimpleNamespace(CORSMiddleware=object))
    sys.modules.setdefault("fastapi.responses", types.SimpleNamespace(JSONResponse=FakeJSONResponse))
    sys.modules.setdefault("playwright", types.SimpleNamespace())
    sys.modules.setdefault(
        "playwright.sync_api",
        types.SimpleNamespace(TimeoutError=FakePlaywrightTimeoutError, sync_playwright=lambda: None),
    )


def load_eod_module():
    install_import_stubs()
    module_path = Path(__file__).resolve().parents[1] / "app" / "main.py"
    spec = importlib.util.spec_from_file_location("eod_main_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MissingLocator:
    @property
    def first(self):
        return self

    def wait_for(self, *args, **kwargs):
        raise FakePlaywrightTimeoutError()


class BlankDoktorAbcShell:
    url = "https://pharmacies.doktorabc.com/end-of-day"

    def locator(self, selector):
        return MissingLocator()

    def wait_for_function(self, *args, **kwargs):
        raise FakePlaywrightTimeoutError()

    def reload(self, *args, **kwargs):
        pass

    def wait_for_load_state(self, *args, **kwargs):
        pass

    def evaluate(self, script):
        return {
            "url": self.url,
            "title": "DoktorABC | Pharmacies",
            "readyState": "complete",
            "emailVisible": False,
            "passwordVisible": False,
            "loginButtonVisible": False,
            "rows100Visible": False,
            "paginationVisible": False,
            "orderMarkerCount": 0,
            "errorText": "",
        }


class AuthRegressionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.eod = load_eod_module()

    def test_blank_end_of_day_shell_is_not_login_form(self):
        page = BlankDoktorAbcShell()

        self.assertFalse(self.eod.visible_login_form(page))

    def test_login_url_alone_is_not_login_form(self):
        page = BlankDoktorAbcShell()
        page.url = "https://pharmacies.doktorabc.com/login"

        self.assertFalse(self.eod.visible_login_form(page))

    def test_strict_login_requires_real_email_field(self):
        page = BlankDoktorAbcShell()

        with self.assertRaisesRegex(RuntimeError, "login form was not visible"):
            self.eod.require_visible_login_form(page)

    def test_login_candidates_try_real_login_pages_before_target_retry(self):
        target_url = "https://pharmacies.doktorabc.com/end-of-day"

        candidates = self.eod.login_url_candidates(target_url)

        self.assertIn("https://pharmacies.doktorabc.com/manage-supplies", candidates)
        self.assertIn("https://pharmacies.doktorabc.com/login", candidates)
        self.assertLess(
            candidates.index("https://pharmacies.doktorabc.com/login"),
            candidates.index(target_url),
        )


if __name__ == "__main__":
    unittest.main()
