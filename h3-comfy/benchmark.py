#!/usr/bin/env python3
"""Validate core nodes/models, submit bounded jobs, save real videos and timing."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import time
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
import uuid

HERE = Path(__file__).resolve().parent


def request(base, path, payload=None, timeout=30):
    data = None if payload is None else json.dumps(payload).encode()
    req = Request(base + path, data=data, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=timeout) as response:
        return json.load(response)


def preflight(graph, info):
    for node in graph.values():
        cls = node["class_type"]
        if cls not in info:
            raise RuntimeError(f"Missing core node: {cls}")
        spec = info[cls]["input"]
        inputs = dict(spec.get("required", {}), **spec.get("optional", {}))
        missing = set(spec.get("required", {})) - node["inputs"].keys()
        if missing:
            raise RuntimeError(f"Missing inputs for {cls}: {sorted(missing)}")
        for key in ("unet_name", "clip_name", "vae_name", "lora_name"):
            if key in node["inputs"]:
                choices = inputs.get(key, [None])[0]
                if not isinstance(choices, list) or node["inputs"][key] not in choices:
                    raise RuntimeError(f"Model not listed by {cls}: {node['inputs'][key]}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resume-prompt")
    parser.add_argument("--resume-started-unix", type=float)
    parser.add_argument("--url", default="http://127.0.0.1:8188")
    parser.add_argument("--workflow", type=Path, default=HERE / "workflow-api.json")
    parser.add_argument("--output", type=Path, default=Path("./h3-results"))
    parser.add_argument("--runs", type=int, choices=[1, 2], default=2)
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--prompt", help="Override multimodal prompt; recorded in submitted graph")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    parsed = urlsplit(args.url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.query or parsed.fragment:
        parser.error("Use an HTTP(S) base URL without query/fragment")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    base = args.url.rstrip("/")
    graph = json.loads(args.workflow.read_text())
    if args.prompt:
        graph["7"]["inputs"]["prompt"] = args.prompt
    stats = request(base, "/system_stats")
    info = request(base, "/object_info")
    preflight(graph, info)
    print("Core nodes and model filenames verified", flush=True)
    if args.check_only:
        print(json.dumps(stats, indent=2))
        return
    args.output.mkdir(parents=True, exist_ok=True)
    result = {"system_stats": stats, "workflow_source": "Massed-Compute/gpu-benchmark PR #33",
              "runs": []}
    (args.output / "system_stats.json").write_text(json.dumps(stats, indent=2) + "\n")
    for index in range(args.runs):
        submitted = copy.deepcopy(graph)
        run_id = uuid.uuid4().hex[:12]
        submitted["16"]["inputs"]["filename_prefix"] = "h3_" + run_id
        (args.output / f"workflow-{index+1}.json").write_text(json.dumps(submitted, indent=2) + "\n")
        if index > 0:
            for node in submitted.values():
                for key in ("seed", "noise_seed"):
                    if key in node["inputs"]:
                        node["inputs"][key] += index
            (args.output / f"workflow-{index+1}.json").write_text(json.dumps(submitted, indent=2) + "\n")
        started = time.monotonic()
        if index == 0 and args.resume_prompt:
            if args.resume_started_unix is None:
                parser.error("Resume requires original start timestamp")
            started -= time.time() - args.resume_started_unix
            response = {"prompt_id": args.resume_prompt}
        else:
            response = request(base, "/prompt", {"prompt": submitted, "client_id": run_id})
        if response.get("error") or response.get("node_errors"):
            raise RuntimeError(f"ComfyUI rejected graph: {response}")
        prompt_id = response["prompt_id"]
        print(f"SUBMITTED {prompt_id}", flush=True)
        try:
            while True:
                history = request(base, "/history/" + prompt_id)
                record = history.get(prompt_id)
                if record:
                    status = record.get("status", {})
                    messages = status.get("messages", [])
                    if status.get("status_str") == "error" or any(
                        m[0] in ("execution_error", "execution_interrupted") for m in messages
                    ):
                        raise RuntimeError(f"ComfyUI execution failed: {record}")
                    if status.get("completed"):
                        break
                if time.monotonic() - started >= args.timeout:
                    raise TimeoutError(f"Prompt timed out: {prompt_id}")
                time.sleep(2)
        except (TimeoutError, KeyboardInterrupt):
            # Run this benchmark on an otherwise idle, single-user instance.
            request(base, "/queue", {"delete": [prompt_id]})
            request(base, "/interrupt", {})
            raise
        elapsed = time.monotonic() - started
        (args.output / f"history-{index+1}.json").write_text(json.dumps(record, indent=2) + "\n")
        videos = []
        for output in record.get("outputs", {}).values():
            for entries in output.values():
                if isinstance(entries, list):
                    videos.extend(item for item in entries if isinstance(item, dict)
                                  and str(item.get("filename", "")).lower().endswith(".mp4"))
        if not videos:
            raise RuntimeError("Completed prompt did not produce an MP4")
        artifacts = []
        for number, item in enumerate(videos):
            query = urlencode({k: item.get(k, "") for k in ("filename", "subfolder", "type")})
            path = args.output / f"run-{index+1}-{number+1}.mp4"
            with urlopen(base + "/view?" + query, timeout=120) as src, path.open("wb") as dest:
                while chunk := src.read(1024 * 1024):
                    dest.write(chunk)
            probe = subprocess.run(["ffprobe", "-v", "error", "-show_streams", "-show_format",
                                    "-of", "json", str(path)], capture_output=True, text=True, check=True)
            metadata = json.loads(probe.stdout)
            streams = metadata.get("streams", [])
            video = next((s for s in streams if s.get("codec_type") == "video"), None)
            audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
            if not video or not audio or float(metadata["format"].get("duration", 0)) <= 0:
                raise RuntimeError("Output must contain valid video and audio")
            if (video.get("width"), video.get("height")) != (1344, 768):
                raise RuntimeError("Output resolution differs from the benchmark")
            artifacts.append({"file": str(path), "bytes": path.stat().st_size,
                              "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                              "ffprobe": metadata})
        result["runs"].append({"prompt_id": prompt_id, "submit_to_history_seconds": round(elapsed, 3),
                               "artifacts": artifacts})
        (args.output / "benchmark.json").write_text(json.dumps(result, indent=2) + "\n")
        print(f"VERIFIED run={index+1} elapsed={elapsed:.3f}s", flush=True)


if __name__ == "__main__":
    main()
