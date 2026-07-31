import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import worker


class _DecodedImage:
    width = 1
    height = 1
    format = "PNG"

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception, _traceback):
        return False

    def load(self):
        return None

    def convert(self, mode):
        if mode != "RGB":
            raise AssertionError(f"unexpected image conversion mode: {mode}")
        return self


class _FakeImage:
    class DecompressionBombError(Exception):
        pass

    decode_error = None

    @classmethod
    def open(cls, _path):
        if cls.decode_error is not None:
            raise cls.decode_error
        return _DecodedImage()


class VisualEncoderImageFailureTests(unittest.TestCase):
    def test_decode_failures_are_invalid_image_and_later_input_still_encodes(self):
        fake_pil = types.ModuleType("PIL")
        fake_pil.Image = _FakeImage
        encoder = worker.VisualEncoder.__new__(worker.VisualEncoder)
        encoder._encode = lambda values, is_query: values

        with tempfile.TemporaryDirectory() as scratch:
            worker.SCRATCH = Path(scratch).resolve()
            image_path = worker.SCRATCH / "image.bin"
            image_path.write_bytes(b"not-an-image")
            value = {
                "kind": "image",
                "path": image_path.name,
                "media_type": "image/png",
                "width": 1,
                "height": 1,
                "encoded_size_bytes": image_path.stat().st_size,
            }

            with patch.dict(sys.modules, {"PIL": fake_pil}):
                for decode_error in (
                    OSError("decode failed"),
                    SyntaxError("decode failed"),
                    ValueError("decode failed"),
                    _FakeImage.DecompressionBombError("decode failed"),
                ):
                    with self.subTest(error_type=type(decode_error).__name__):
                        _FakeImage.decode_error = decode_error
                        with self.assertRaises(worker.ProtocolError) as raised:
                            encoder.encode_documents([value])
                        self.assertEqual(raised.exception.code, "invalid_image")

                        _FakeImage.decode_error = None
                        self.assertEqual(len(encoder.encode_documents([value])), 1)


if __name__ == "__main__":
    unittest.main()
