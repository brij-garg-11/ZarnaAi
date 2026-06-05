"""
Tests for operator image-upload validation (Item 2).

The validator lives in the operator service (operator/app/image_validation.py),
whose `app` package collides with the main app's. It has no DB dependency, so we
load it directly by file path and exercise it with real PIL-generated images.
"""
import importlib.util
import io
import os

import pytest

_MODULE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "operator", "app", "image_validation.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("operator_image_validation", _MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


imgval = _load()
PIL = pytest.importorskip("PIL")
from PIL import Image  # noqa: E402


def _png(w, h):
    buf = io.BytesIO()
    Image.new("RGB", (w, h), (200, 100, 50)).save(buf, "PNG")
    return buf.getvalue()


class TestValidateImageBytes:
    def test_square_ok(self):
        assert imgval.validate_image_bytes(_png(640, 640)) is None

    def test_near_square_within_tolerance_ok(self):
        assert imgval.validate_image_bytes(_png(640, 638)) is None

    def test_default_allows_non_square(self):
        # Square is relaxed by default — landscape/portrait uploads are accepted.
        assert imgval.validate_image_bytes(_png(1200, 800)) is None

    def test_non_square_rejected_when_square_required(self):
        err = imgval.validate_image_bytes(_png(1200, 800), require_square=True)
        assert err and "square" in err.lower()
        assert "1200" in err and "800" in err

    def test_empty_rejected(self):
        assert "empty" in imgval.validate_image_bytes(b"")

    def test_oversized_rejected(self):
        big = _png(50, 50) + b"\x00" * (imgval.MAX_UPLOAD_BYTES + 10)
        err = imgval.validate_image_bytes(big)
        assert err and "5 MB" in err

    def test_not_an_image_rejected(self):
        err = imgval.validate_image_bytes(b"definitely not an image")
        assert err and "valid image" in err.lower()

    def test_require_square_false_allows_landscape(self):
        assert imgval.validate_image_bytes(_png(1200, 800), require_square=False) is None
