"""Optional media observations; runtime nodes determine input compatibility."""
from fractions import Fraction
import json
import math
import subprocess


def probe(path, command="ffprobe"):
    # Read container metadata only. No decoding, frame counting or model execution.
    result = subprocess.run([command or "ffprobe", "-v", "error", "-show_streams",
        "-show_format", "-of", "json", str(path)], capture_output=True, check=True, timeout=30)
    if len(result.stdout) > 1024 * 1024:
        raise ValueError("media metadata exceeds bound")
    info = json.loads(result.stdout)
    videos = [s for s in info.get("streams", []) if s.get("codec_type") == "video"]
    audios = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
    if len(videos) != 1 or len(audios) > 1:
        raise ValueError("expected one video/image stream and at most one audio stream")
    v = videos[0]
    return {"width": int(v.get("width", 0)), "height": int(v.get("height", 0)),
        "frames": int(v.get("nb_frames") or 0), "fps": v.get("avg_frame_rate") or "0/1",
        "r_fps": v.get("r_frame_rate") or "0/1", "start": float(v.get("start_time") or 0),
        "audio_start": float(audios[0].get("start_time") or 0) if audios else 0,
        "duration": float(v.get("duration") or info.get("format", {}).get("duration") or 0),
        "has_audio": bool(audios),
        "audio_duration": float(audios[0].get("duration") or 0) if audios else 0}


def lanpaint_error(task):
    """Advisory only: upstream operates on actual inputs, not benchmark dimensions."""
    facts = task.get("_runpod_media") or {}
    refs = task.get("references") or []
    if any((facts.get(r["asset_id"]) or {}).get("sha256") != r.get("sha256") for r in refs):
        return "部分素材媒体信息未取得；将使用实际素材执行，节点可能报告格式或音轨错误。"
    if len(refs) > 3 and not facts[refs[3]["asset_id"]].get("has_audio"):
        return "编码视频未探测到音轨；仍尝试实际节点，AVEncode可能报告缺少音轨。"
    return None
