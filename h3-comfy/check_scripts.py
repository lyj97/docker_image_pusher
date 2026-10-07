#!/usr/bin/env python3
"""Offline checks for integrity failures and billed-job cancellation; no cloud calls."""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


prepare = load("prepare_models")
bench = load("benchmark")


class PreparationChecks(unittest.TestCase):
    def test_startup_rejects_old_runtime_before_model_download(self):
        # Exercise the actual HTTP readiness gate with disposable responses.
        startup = (HERE / 'official-startup.sh').read_text()
        code = startup.split('python3 - "$base_pid" <<\'PY\'\n', 1)[1].split('\nPY', 1)[0]
        class Reply(io.BytesIO):
            status = 200
        for version in ('0.30.0', '0.39.0'):
            reply = Reply(json.dumps({'system': {'comfyui_version': version}}).encode())
            with patch('urllib.request.urlopen', return_value=reply), \
                    patch('os.kill'), patch.dict(os.environ, H3_READY_TIMEOUT='10', H3_COMFY_VERSION='0.39.0'), \
                    patch.object(sys, 'argv', ['readiness', '123']):
                if version == '0.30.0':
                    with self.assertRaisesRegex(SystemExit, 'version mismatch'):
                        exec(compile(code, 'startup-readiness', 'exec'), {})
                else:
                    exec(compile(code, 'startup-readiness', 'exec'), {})

    def test_startup_failure_records_stop_signal_and_app_log(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'comfy/user').mkdir(parents=True)
            (root / 'comfy/user/comfyui_8188.log').write_text('fixture application log\n')
            env = dict(os.environ, PUBLIC_KEY='', H3_RUN_ROOT=str(root / 'runs'),
                       H3_STORAGE_ROOT=str(root / 'storage'), H3_COMFY_DIR=str(root / 'comfy'))
            result = subprocess.run(['bash', str(HERE / 'official-startup.sh')],
                                    env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            run = Path((root / 'runs/current-run.txt').read_text().strip())
            self.assertTrue((run / 'status.txt').read_text().startswith('FAILED'))
            self.assertEqual((run / 'stop-required.txt').read_text(), 'STOP_REQUIRED\n')
            self.assertEqual((run / 'comfyui.log').read_text(), 'fixture application log\n')

    def test_local_mode_streams_hash_without_readback(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data = b"local model"
            manifest = root / "lock.json"
            manifest.write_text(json.dumps({"global_volume_id": "historical", "models": [{
                "repo": "fixture/model", "revision": "abc", "source": "x", "target": "vae/x",
                "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}]}))
            args = ["prepare", "--local", "--manifest", str(manifest), "--storage-root", str(root),
                    "--metrics-dir", str(root / "metrics")]
            with patch.object(sys, "argv", args), patch.object(prepare, "urlopen", return_value=io.BytesIO(data)), patch.object(prepare.shutil, "disk_usage", return_value=SimpleNamespace(free=10**10)):
                prepare.main()
            ready = json.loads((root / "h3/models-ready.json").read_text())
            self.assertEqual(ready["storage_mode"], "local")
            self.assertFalse(ready["readback_verified"])
            report = json.loads(next((root / "metrics").glob("*.json")).read_text())
            self.assertEqual([e["phase"] for e in report["events"]], ["download_and_write"])

    def test_direct_download_integrity_and_idempotence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            contents = b"tiny model fixture"
            manifest = root / "lock.json"
            manifest.write_text(json.dumps({"global_volume_id": "fixture", "models": [{
                "repo": "fixture/model", "revision": "abc", "source": "x", "target": "vae/x",
                "bytes": len(contents), "sha256": hashlib.sha256(contents).hexdigest()}]}))
            argv = ["prepare", "--manifest", str(manifest), "--storage-root", str(root),
                    "--allow-unmounted", "--direct"]
            with patch.object(sys, "argv", argv), patch.object(prepare, "urlopen", return_value=io.BytesIO(contents)):
                prepare.main()
            marker = root / "h3/models-ready.json"
            self.assertTrue(marker.exists())
            with patch.object(sys, "argv", argv), patch.object(prepare, "urlopen", side_effect=AssertionError("should reuse verified file")):
                prepare.main()
            (root / "h3/models/vae/x").write_bytes(b"corrupt fixture")
            with patch.object(sys, "argv", argv), patch.object(prepare, "urlopen", return_value=io.BytesIO(b"bad")):
                with self.assertRaises(RuntimeError):
                    prepare.main()
            self.assertFalse(marker.exists(), "failed integrity check must revoke readiness")

    def test_unmounted_storage_refused_before_writes(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(sys, "argv", ["prepare", "--storage-root", temp]):
                with self.assertRaises(RuntimeError):
                    prepare.main()
            self.assertEqual(list(Path(temp).iterdir()), [])


class BenchmarkChecks(unittest.TestCase):
    def info(self):
        graph = json.loads((HERE / "workflow-api.json").read_text())
        result = {}
        for node in graph.values():
            required = {}
            for k, v in node["inputs"].items():
                required[k] = [[v]] if k.endswith("_name") else ["ANY"]
            cls = node["class_type"]
            if cls in result:
                for key, spec in required.items():
                    if key.endswith("_name"):
                        result[cls]["input"]["required"][key][0].extend(spec[0])
            else:
                result[cls] = {"input": {"required": required}}
        return result

    def test_execution_error_fails_without_second_submission(self):
        calls = []
        def respond(base, path, payload=None):
            calls.append(path)
            if path == "/system_stats": return {}
            if path == "/object_info": return self.info()
            if path == "/prompt": return {"prompt_id": "fixture"}
            if path.startswith("/history/"):
                return {"fixture": {"status": {"status_str": "error", "messages": [["execution_error", {}]]}}}
            raise AssertionError(path)
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(sys, "argv", ["bench", "--output", temp]), patch.object(bench, "request", side_effect=respond):
                with self.assertRaisesRegex(RuntimeError, "execution failed"):
                    bench.main()
        self.assertEqual(calls.count("/prompt"), 1)

    def test_timeout_removes_queued_job_and_interrupts(self):
        calls = []
        def respond(base, path, payload=None):
            calls.append((path, payload))
            if path == "/system_stats": return {}
            if path == "/object_info": return self.info()
            if path == "/prompt": return {"prompt_id": "fixture"}
            return {}
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(sys, "argv", ["bench", "--output", temp, "--timeout", "1"]), \
                    patch.object(bench, "request", side_effect=respond), \
                    patch.object(bench.time, "monotonic", side_effect=[0, 2]):
                with self.assertRaises(TimeoutError):
                    bench.main()
        self.assertIn(("/queue", {"delete": ["fixture"]}), calls)
        self.assertIn(("/interrupt", {}), calls)


if __name__ == "__main__":
    with contextlib.redirect_stdout(io.StringIO()):
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    sys.exit(0 if result.wasSuccessful() else 1)
