#!/usr/bin/env python3
"""MLX TTS adapter that speaks the H3 worker machine-event contract."""

from __future__ import annotations

import argparse
import json
import os
import sys
import wave


def emit(kind: str, **fields) -> None:
    print(json.dumps({"type": kind, **fields}, ensure_ascii=False), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("cosyvoice3", "qwen3"),
                        default="cosyvoice3")
    parser.add_argument("--operation",
                        choices=("clone", "voice_design", "custom_voice"),
                        default="clone")
    parser.add_argument("--model", required=True)
    parser.add_argument("--text", required=True)
    parser.add_argument("--ref-audio")
    parser.add_argument("--ref-text")
    parser.add_argument("--instruct")
    parser.add_argument("--voice")
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--speed", type=float, default=1.0)
    parser.add_argument("--language", default="zh")
    parser.add_argument("--temperature", type=float, default=0.7)
    args = parser.parse_args()

    try:
        emit("phase", phase="tts model load")
        from mlx_audio.tts.generate import generate_audio

        emit("phase", phase="tts generate")
        if args.operation == "clone" and not args.ref_audio:
            raise ValueError("clone requires --ref-audio")
        if args.operation == "voice_design" and not args.instruct:
            raise ValueError("voice_design requires --instruct")
        if args.operation == "custom_voice" and not args.voice:
            raise ValueError("custom_voice requires --voice")
        language = args.language
        if args.backend == "qwen3":
            language = {"zh": "chinese", "en": "english"}.get(
                language.lower(), language
            )
        kwargs = {
            "text": args.text,
            "model": args.model,
            "ref_audio": args.ref_audio,
            "ref_text": args.ref_text,
            "file_prefix": os.path.splitext(os.path.abspath(args.output))[0],
            "audio_format": "wav",
            "join_audio": True,
            "play": False,
            "verbose": False,
            "seed": args.seed,
            "speed": args.speed,
            "lang_code": language,
            "temperature": args.temperature,
        }
        if args.backend == "qwen3":
            kwargs["instruct"] = args.instruct
            kwargs["voice"] = args.voice
        else:
            kwargs["instruct_text"] = args.instruct
        generate_audio(
            **kwargs,
        )
        if not os.path.isfile(args.output) or os.path.getsize(args.output) <= 44:
            raise RuntimeError("TTS backend produced no WAV output")
        with wave.open(args.output, "rb") as wav:
            sample_rate = wav.getframerate()
            channels = wav.getnchannels()
            samples = wav.getnframes()
        if sample_rate <= 0 or channels <= 0 or samples <= 0:
            raise RuntimeError("TTS backend produced an invalid WAV file")
        emit("result", result={
            "sample_rate": sample_rate,
            "channels": channels,
            "samples": samples,
            "duration_seconds": samples / sample_rate,
            "seed": str(args.seed),
        })
        emit("done")
        return 0
    except Exception as exc:
        emit("error", message=str(exc)[:1000])
        return 1


if __name__ == "__main__":
    sys.exit(main())
