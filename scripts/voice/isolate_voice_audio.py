"""
Run audio clips through ElevenLabs' Audio Isolation API to strip background
noise (crowd, applause, music) before a Professional Voice Clone upload.

This is the automated version of clicking through Voice Isolator in the UI: it
sends each input file to the isolation endpoint and saves a cleaned `*_isolated.mp3`
next to it (or in --out-dir). It reads ELEVENLABS_API_KEY from the environment
(.env is loaded automatically) and never prints the key.

Cost note: isolation spends ElevenLabs credits (roughly ~1000 credits per minute
of audio). Only run it on the clips you actually intend to upload.

Usage:
    # isolate specific clips
    python scripts/voice/isolate_voice_audio.py \
        ~/Downloads/zarna_voice_samples/*_part001.mp3 \
        ~/Downloads/zarna_voice_samples/*_part002.mp3

    # isolate everything in a folder
    python scripts/voice/isolate_voice_audio.py ~/Downloads/zarna_voice_samples/

    # custom output directory
    python scripts/voice/isolate_voice_audio.py <files...> --out-dir ~/Downloads/zarna_clean
"""
from __future__ import annotations

import argparse
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv()

import requests

API_URL = "https://api.elevenlabs.io/v1/audio-isolation"
AUDIO_EXTS = (".mp3", ".wav", ".m4a", ".flac", ".aac", ".ogg")


def _collect_inputs(paths: list[str]) -> list[str]:
    """Expand any directories in the input list into their audio files."""
    files: list[str] = []
    for p in paths:
        ap = os.path.abspath(os.path.expanduser(p))
        if os.path.isdir(ap):
            for name in sorted(os.listdir(ap)):
                if name.lower().endswith(AUDIO_EXTS) and "_isolated" not in name:
                    files.append(os.path.join(ap, name))
        elif os.path.isfile(ap):
            files.append(ap)
        else:
            print(f"warning: skipping missing path: {ap}", file=sys.stderr)
    return files


def _isolate_one(src: str, dest: str, api_key: str, max_retries: int = 5) -> bool:
    """Send one file to the isolation API and stream the result to dest."""
    delay = 10
    for attempt in range(1, max_retries + 1):
        try:
            with open(src, "rb") as fh:
                resp = requests.post(
                    API_URL,
                    headers={"xi-api-key": api_key},
                    files={"audio": (os.path.basename(src), fh, "audio/mpeg")},
                    timeout=600,
                    stream=True,
                )
        except requests.RequestException as exc:
            print(f"  network error (attempt {attempt}/{max_retries}): {exc}", file=sys.stderr)
            time.sleep(delay)
            delay *= 2
            continue

        if resp.status_code == 200:
            with open(dest, "wb") as out:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        out.write(chunk)
            return True

        if resp.status_code == 429:
            print(f"  rate limited (attempt {attempt}/{max_retries}); backing off {delay}s", file=sys.stderr)
            time.sleep(delay)
            delay *= 2
            continue

        # non-retryable
        body = (resp.text or "")[:300]
        print(f"  error {resp.status_code}: {body}", file=sys.stderr)
        return False

    print("  giving up after retries", file=sys.stderr)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("inputs", nargs="+", help="audio files and/or directories to isolate")
    parser.add_argument("--out-dir", default=None, help="output directory (default: alongside each source file)")
    args = parser.parse_args()

    api_key = (os.getenv("ELEVENLABS_API_KEY") or "").strip()
    if not api_key:
        print("error: ELEVENLABS_API_KEY not set (add it to .env)", file=sys.stderr)
        return 2

    files = _collect_inputs(args.inputs)
    if not files:
        print("error: no audio files found to isolate", file=sys.stderr)
        return 2

    if args.out_dir:
        out_dir = os.path.abspath(os.path.expanduser(args.out_dir))
        os.makedirs(out_dir, exist_ok=True)
    else:
        out_dir = None

    print(f"isolating {len(files)} file(s) via ElevenLabs Audio Isolation...")
    ok = 0
    for i, src in enumerate(files, 1):
        base = os.path.splitext(os.path.basename(src))[0]
        target_dir = out_dir or os.path.dirname(src)
        dest = os.path.join(target_dir, f"{base}_isolated.mp3")
        print(f"[{i}/{len(files)}] {os.path.basename(src)} -> {os.path.basename(dest)}")
        if _isolate_one(src, dest, api_key):
            size_mb = os.path.getsize(dest) / (1024 * 1024)
            print(f"  ok ({size_mb:.1f} MB)")
            ok += 1
        else:
            print("  failed")

    print(f"done: {ok}/{len(files)} isolated, in {out_dir or 'source folders'}")
    if ok:
        print("next: review the *_isolated.mp3 files, then upload them to the Professional Voice Clone.")
    return 0 if ok == len(files) else 1


if __name__ == "__main__":
    raise SystemExit(main())
