"""Probe downloaded media independently of remote metadata before publication."""
from fractions import Fraction
import json
import math
import subprocess


def video_result(path, ffprobe, task):
    process = subprocess.run([ffprobe or 'ffprobe', '-v', 'error', '-count_frames',
        '-show_streams', '-show_format', '-of', 'json', str(path)],
        capture_output=True, timeout=None, check=True)
    if len(process.stdout) > 1024 * 1024:
        raise ValueError('media probe exceeds bound')
    data = json.loads(process.stdout)
    videos = [s for s in data.get('streams', []) if s.get('codec_type') == 'video']
    audios = [s for s in data.get('streams', []) if s.get('codec_type') == 'audio']
    if len(videos) != 1 or len(audios) != 1:
        raise ValueError('H3 profile requires one video and one audio stream')
    v = videos[0]
    audio_duration = float(audios[0].get('duration') or 0)
    audio_start = float(audios[0].get('start_time') or 0)
    from shared.execution import generation_specification
    specification = generation_specification(task)
    width, height = int(v['width']), int(v['height'])
    frames = int(v['nb_read_frames'])
    fps = float(Fraction(v['avg_frame_rate']))
    duration = float(v.get('duration') or data['format']['duration'])
    if (width != specification['width'] or height != specification['height']
            or frames != specification['length'] or fps != 24
            or not math.isfinite(duration) or abs(duration - frames / fps) > 1 / fps):
        raise ValueError('media differs from reviewed generation specification')
    if (not math.isfinite(audio_duration) or not math.isfinite(audio_start)
            or audio_duration <= 0 or abs(audio_duration - duration) > 1 / fps
            or abs(audio_start) > 1 / fps):
        raise ValueError('audio timeline differs from video specification')
    return {'audio_duration_seconds': audio_duration, 'width': width, 'height': height, 'frames': frames, 'fps': fps,
            'duration_seconds': duration, 'output': str(path)}


def original_soundtrack(path, source, task, config):
    """Preserve the business a2va policy using the service's own AAC encoder."""
    from pathlib import Path
    from h3worker.runner import verify_a2va_mux
    from h3worker.worker import file_digest
    if file_digest(source)[1] != task['references'][0]['sha256']:
        raise ValueError('a2va source digest differs from admission')
    temporary = Path(path).with_name('soundtrack.pending.mp4')
    duration = task['generation']['frames'] / 24
    try:
        subprocess.run([config.ffmpeg_path or 'ffmpeg', '-v', 'error', '-y', '-i', str(path),
            '-i', str(source), '-map', '0:v:0', '-map', '1:a:0', '-c:v', 'copy',
            '-af', 'asetpts=PTS-STARTPTS,apad', '-t', f'{duration:.9f}', '-c:a', 'aac',
            '-b:a', '192k', '-movflags', '+faststart', str(temporary)],
            capture_output=True, timeout=None, check=True)
        verify_a2va_mux(str(temporary), str(source), task['generation'], config)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
