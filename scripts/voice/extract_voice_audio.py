"""
Extract and pre-clean audio from a video for an ElevenLabs voice clone.

ElevenLabs wants clean, isolated speech (not a 2 GB video). This script pulls
the audio track out of a video file, downmixes to mono, and writes a compact
file that is ready to upload for a Professional Voice Clone. Optionally it can:

  * apply a light denoise / band-pass pass to tame room and crowd noise, and
  * split the result into fixed-length chunks so you can drop the
    laughter-heavy segments before uploading (handy for live-special audio).

It never touches the source file and never uploads anything — it only produces
local audio files for you to review and hand to ElevenLabs.

Usage:
    python scripts/voice/extract_voice_audio.py "/path/to/special.mp4"

    # split into 10-minute chunks and apply a light clean-up pass
    python scripts/voice/extract_voice_audio.py "/path/to/special.mp4" \
        --chunk-minutes 10 --denoise

    # only keep a clean stretch (mm:ss or seconds), e.g. 5:00 to 40:00
    python scripts/voice/extract_voice_audio.py "/path/to/special.mp4" \
        --start 5:00 --end 40:00

Notes:
  * Simple denoise will NOT remove audience laughter/applause (those are
    speech-like). For that, run the output through ElevenLabs' Voice Isolator,
    or trim the laughter-heavy chunks out manually.
  * A clone trained on stage audio keeps that projected, performing energy.
    Mixing in conversational audio (podcast/interview) gives a more natural
    phone voice.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys


def _require_tool(name: str) -> None:
    if shutil.which(name) is None:
        print(f"error: '{name}' not found on PATH — install ffmpeg (brew install ffmpeg)", file=sys.stderr)
        raise SystemExit(2)


def _parse_timecode(value: str | None) -> str | None:
    """Accept seconds ('330') or mm:ss / hh:mm:ss and return an ffmpeg timecode."""
    if not value:
        return None
    value = value.strip()
    if ":" not in value:
        # plain seconds — let ffmpeg interpret the bare number
        return value
    return value


def _probe_audio(path: str) -> dict:
    """Return basic audio stream info via ffprobe (best-effort)."""
    try:
        out = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "a:0",
                "-show_entries", "stream=sample_rate,channels:format=duration",
                "-of", "default=noprint_wrappers=1:nokey=0",
                path,
            ],
            capture_output=True, text=True, check=True,
        ).stdout
    except subprocess.CalledProcessError:
        return {}
    info: dict = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            info[k.strip()] = v.strip()
    return info


def _build_filters(denoise: bool) -> str | None:
    """Light, conservative clean-up. Kept gentle so the clone stays faithful."""
    if not denoise:
        return None
    # highpass removes low rumble/handling; lowpass trims hiss above speech;
    # afftdn is a mild FFT denoiser. Conservative settings on purpose.
    return "highpass=f=80,lowpass=f=12000,afftdn=nf=-25"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", help="path to the source video/audio file")
    parser.add_argument("--out-dir", default=None, help="output directory (default: <input dir>/zarna_voice_samples)")
    parser.add_argument("--format", choices=["mp3", "wav", "m4a"], default="mp3", help="output audio format (default: mp3)")
    parser.add_argument("--bitrate", default="192k", help="output bitrate for lossy formats (default: 192k)")
    parser.add_argument("--sample-rate", type=int, default=0, help="resample to this rate in Hz (default: keep source)")
    parser.add_argument("--chunk-minutes", type=int, default=0, help="split output into N-minute chunks (0 = single file)")
    parser.add_argument("--denoise", action="store_true", help="apply a light denoise / band-pass clean-up pass")
    parser.add_argument("--start", default=None, help="trim start (seconds or mm:ss / hh:mm:ss)")
    parser.add_argument("--end", default=None, help="trim end (seconds or mm:ss / hh:mm:ss)")
    args = parser.parse_args()

    _require_tool("ffmpeg")
    _require_tool("ffprobe")

    src = os.path.abspath(os.path.expanduser(args.input))
    if not os.path.isfile(src):
        print(f"error: input not found: {src}", file=sys.stderr)
        return 2

    out_dir = args.out_dir or os.path.join(os.path.dirname(src), "zarna_voice_samples")
    os.makedirs(out_dir, exist_ok=True)

    info = _probe_audio(src)
    if info:
        dur = float(info.get("duration", 0) or 0)
        print(
            f"source audio: {info.get('sample_rate', '?')} Hz, "
            f"{info.get('channels', '?')} channel(s), {dur/60:.1f} min"
        )

    base = os.path.splitext(os.path.basename(src))[0]
    ext = args.format

    # common encode args: drop video, downmix to mono
    common: list[str] = ["-vn", "-ac", "1"]
    if args.sample_rate > 0:
        common += ["-ar", str(args.sample_rate)]
    if ext in ("mp3", "m4a"):
        common += ["-b:a", args.bitrate]

    filters = _build_filters(args.denoise)
    if filters:
        common += ["-af", filters]

    trim: list[str] = []
    start = _parse_timecode(args.start)
    end = _parse_timecode(args.end)
    if start:
        trim += ["-ss", start]
    if end:
        trim += ["-to", end]

    cmd = ["ffmpeg", "-y", "-i", src, *trim, *common]

    if args.chunk_minutes > 0:
        seg_seconds = args.chunk_minutes * 60
        out_pattern = os.path.join(out_dir, f"{base}_part%03d.{ext}")
        cmd += ["-f", "segment", "-segment_time", str(seg_seconds), out_pattern]
        target_desc = f"chunks of {args.chunk_minutes} min -> {out_pattern}"
    else:
        out_path = os.path.join(out_dir, f"{base}.{ext}")
        cmd += [out_path]
        target_desc = out_path

    print(f"writing: {target_desc}")
    if args.denoise:
        print("note: light denoise applied — this does NOT remove audience laughter/applause.")

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        print(f"error: ffmpeg failed (exit {exc.returncode})", file=sys.stderr)
        return 1

    produced = sorted(f for f in os.listdir(out_dir) if f.startswith(base) and f.endswith(ext))
    total_mb = sum(os.path.getsize(os.path.join(out_dir, f)) for f in produced) / (1024 * 1024)
    print(f"done: {len(produced)} file(s), {total_mb:.1f} MB total, in {out_dir}")
    print("next: review/trim the clips, run them through ElevenLabs Voice Isolator if needed, then upload to the Professional Voice Clone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
