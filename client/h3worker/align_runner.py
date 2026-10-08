#!/usr/bin/env python3
"""Qwen3 ForcedAligner adapter for the H3 worker event contract."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_SERVICE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _SERVICE_ROOT not in sys.path:
    sys.path.insert(0, _SERVICE_ROOT)

from shared.h3proto import validate_alignment_payload


def emit(kind: str, **fields) -> None:
    print(json.dumps({"type": kind, **fields}, ensure_ascii=False), flush=True)


def _counts(payload: dict) -> tuple[int, int]:
    segments = payload.get("segments")
    if isinstance(segments, list):
        tokens = sum(
            len(item.get("words") or []) or 1
            for item in segments if isinstance(item, dict)
        )
        return len(segments), tokens
    return 0, 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--audio", required=True)
    parser.add_argument("--text", required=True)
    parser.add_argument("--language", default="Chinese")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    try:
        emit("phase", phase="tts model load")
        from mlx_audio.stt.generate import generate_transcription

        output = Path(args.output).resolve()
        prefix = output.with_suffix("")
        emit("phase", phase="tts generate")
        generate_transcription(
            model=args.model,
            audio=args.audio,
            output_path=str(prefix),
            format="json",
            text=args.text,
            language=args.language,
            verbose=False,
        )
        generated = Path(str(prefix) + ".json")
        if not generated.is_file() or generated.stat().st_size == 0:
            raise RuntimeError("Qwen3 ForcedAligner produced no JSON output")
        payload = json.loads(generated.read_text(encoding="utf-8"))
        problems = validate_alignment_payload(payload, requested_text=args.text)
        if problems:
            raise RuntimeError(
                "Qwen3 ForcedAligner produced invalid JSON output: "
                + "; ".join(problems[:3])
            )
        if generated != output:
            os.replace(generated, output)
        segments, tokens = _counts(payload)
        emit("result", result={
            "text": payload["text"],
            "segments": segments,
            "tokens": tokens,
        })
        emit("done")
        return 0
    except Exception as exc:
        emit("error", message=str(exc)[:1000])
        return 1


if __name__ == "__main__":
    sys.exit(main())
