import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location("local_inference",pathlib.Path(__file__).resolve().parents[1]/"scripts/local_inference.py")
mod=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

class IntegrityTests(unittest.TestCase):
    def test_curated_input_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d)
            for f,body in {"model-lock.json":"{}", "system.txt":"bounded proposal", "memory.json":"{}", "sources.json":"{}"}.items():
                (p/f).write_text(body)
            mapping={"model_lock":"model-lock.json","system":"system.txt","memory":"memory.json","sources":"sources.json"}
            (p/"input-lock.json").write_text(json.dumps({k:mod.digest(p/v) for k,v in mapping.items()}))
            with patch.object(mod,"STATE",p),patch.object(mod,"CONFIG",p):
                mod.checked_inputs()
                (p/"memory.json").write_text('{"unauthorized":"new memory"}')
                with self.assertRaisesRegex(ValueError,"memory"):mod.checked_inputs()

    def test_same_length_weight_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d);(p/"models").mkdir()
            model=p/"models/model.gguf";model.write_bytes(b"original")
            lock={"filename":"model.gguf","bytes":8,"sha256":mod.digest(model)}
            with patch.object(mod,"STATE",p):
                self.assertEqual(mod.verify_model(lock),model)
                model.write_bytes(b"modified")
                with self.assertRaisesRegex(ValueError,"checksum"):mod.verify_model(lock)
