from __future__ import annotations

import argparse
import urllib.request
import zipfile
from pathlib import Path

from src.common.config import load_config


def download_public_dataset(config_path: str = "config/pipeline.yml", overwrite: bool = False) -> Path:
    config = load_config(config_path)
    config.source_path.mkdir(parents=True, exist_ok=True)
    archive_path = config.source_path / config.dataset_archive_file
    excel_path = config.source_path / config.dataset_excel_file

    if not archive_path.exists() or overwrite:
        urllib.request.urlretrieve(config.dataset_url, archive_path)

    if not excel_path.exists() or overwrite:
        with zipfile.ZipFile(archive_path) as archive:
            archive.extract(config.dataset_excel_file, config.source_path)

    return excel_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download the UCI Online Retail public dataset.")
    parser.add_argument("--config", default="config/pipeline.yml", help="Pipeline config path.")
    parser.add_argument("--overwrite", action="store_true", help="Redownload and re-extract files.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    path = download_public_dataset(config_path=args.config, overwrite=args.overwrite)
    print(f"Public dataset available at {path}")


if __name__ == "__main__":
    main()
