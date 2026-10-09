"""Fixed LanPaint media contract, evaluated before paid compute; no conversion."""
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
    facts = task.get("_runpod_media") or {}
    refs = task.get("references") or []
    if len(refs) != 4:
        return "LanPaint需要四份已核对素材"
    for i, ref in enumerate(refs):
        f = facts.get(ref["asset_id"], {})
        if f.get("sha256") != ref.get("sha256"):
            return "素材媒体信息缺失或与服务器对象身份不符，须在开机前核对"
        if f.get("width") != 256 or f.get("height") != 256:
            return "当前LanPaint配置要求所有素材为256×256，不会自动缩放素材"
        if i == 1:
            continue
        try:
            if f.get("frames") != 39 or Fraction(f.get("fps", "0")) != 24 or Fraction(f.get("r_fps", "0")) != 24:
                return "当前LanPaint视频素材须为39帧、24fps，时间轴必须一致"
            start = float(f["start"])
            if not math.isfinite(start) or abs(start) > 1 / 24:
                return "LanPaint素材视频需要从统一零点开始"
            duration = float(f["duration"])
            if not math.isfinite(duration) or abs(duration - 39 / 24) > 1 / 24:
                return "LanPaint视频时间轴不符合39帧、24fps"
            if i in (0, 3):
                ad = float(f.get("audio_duration", 0))
                audio_start = float(f.get("audio_start", 0))
                if (not f.get("has_audio") or not math.isfinite(ad) or abs(ad - duration) > 1 / 24
                        or not math.isfinite(audio_start) or abs(audio_start - start) > 1 / 24):
                    return "LanPaint源视频和编码视频需要与视频时间轴一致的音轨"
        except (ValueError, TypeError, KeyError, ZeroDivisionError):
            return "素材媒体元数据无效，须在开机前核对"
    return None
