"""Shared helper: download a Kaggle dataset into data/raw, idempotently.

Uses the official Kaggle API (https://github.com/Kaggle/kaggle-api).
Login: `.venv/bin/kaggle auth login`, or a token in ~/.kaggle/access_token,
or the KAGGLE_API_TOKEN environment variable.

Idempotent: if every expected file is already present, nothing is downloaded,
so re-running is always safe (the same property every pipeline step will need).
"""
from pathlib import Path
import sys

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def download(dataset: str, expected: list[str]) -> int:
    """Download `dataset` (owner/name) into data/raw unless `expected` files exist.

    Returns a process exit code: 0 on success, 1 if files are missing afterwards.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    def missing() -> list[str]:
        return [f for f in expected if not (RAW_DIR / f).exists()]

    if not missing():
        print(f"{dataset}: all {len(expected)} files already in {RAW_DIR}, nothing to do.")
        return 0

    # Imported late so the "nothing to do" path works even without a Kaggle login.
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    print(f"Downloading {dataset} into {RAW_DIR} ...")
    api.dataset_download_files(dataset, path=str(RAW_DIR), unzip=True, quiet=False)

    still_missing = missing()
    if still_missing:
        print(f"Missing after download: {still_missing}", file=sys.stderr)
        print("Files now in data/raw:", sorted(p.name for p in RAW_DIR.glob("*.csv")), file=sys.stderr)
        return 1
    for f in expected:
        size_mb = (RAW_DIR / f).stat().st_size / 1_000_000
        print(f"  {f:45s} {size_mb:7.1f} MB")
    return 0
