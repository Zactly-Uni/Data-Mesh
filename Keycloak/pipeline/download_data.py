#!/usr/bin/env python3
#download_data.py

#Downloads Solar-Kataster Hessen data as ZIPs, stores locally, and writes a JSONL manifest
#with mapped attributes (mapping happens here).

#Because the site is a WebGIS app, the real ZIP endpoint may be created dynamically.
#This script supports:
#  - direct mode: download ZIPs from a known URL template
#  - playwright mode: automate the website (may need selector tweaks)

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests


BASE_PAGE = "https://solar-kataster-hessen.de/appsk2/pv/hessensolar_download.html"


@dataclass
class ManifestRecord:
    downloaded_at_utc: str
    source_page: str
    source_zip_url: str
    region: str
    dataset: str  # e.g. "anzeigeraster" or "abfrageraster"
    local_zip_path: str
    bytes: int
    sha256: str
    http_status: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\-]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "region"


def download_zip(url: str, out_path: Path, timeout_s: int = 120) -> tuple[int, int]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout_s) as r:
        status = r.status_code
        r.raise_for_status()
        total = 0
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 512):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
        tmp.replace(out_path)
    return status, total


def append_manifest(manifest_path: Path, rec: ManifestRecord) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")


def direct_mode(region: str, dataset: str, url_template: str, out_dir: Path, manifest: Path) -> None:
    """
    url_template example (you will adapt once you know the real endpoint):
      "https://solar-kataster-hessen.de/appsk2/pv/EXPORT?dataset={dataset}&region={region}"
    """
    zip_url = url_template.format(dataset=dataset, region=region)
    filename = f"{safe_slug(region)}__{dataset}__{int(time.time())}.zip"
    out_path = out_dir / filename

    print(f"[download] region={region} dataset={dataset}")
    print(f"[download] url={zip_url}")
    status, nbytes = download_zip(zip_url, out_path)

    digest = sha256_file(out_path)
    rec = ManifestRecord(
        downloaded_at_utc=utc_now_iso(),
        source_page=BASE_PAGE,
        source_zip_url=zip_url,
        region=region,
        dataset=dataset,
        local_zip_path=str(out_path.resolve()),
        bytes=nbytes,
        sha256=digest,
        http_status=status,
    )
    append_manifest(manifest, rec)
    print(f"[ok] saved={out_path} bytes={nbytes} sha256={digest}")


def playwright_mode_stub(region: str, dataset: str, out_dir: Path, manifest: Path) -> None:
    """
    Scaffold. In practice you may need to tweak selectors.

    Plan:
      - open BASE_PAGE
      - select/click municipality or use search widget (if present)
      - choose dataset (anzeige/abfrage)
      - click "ZIP-Archiv erstellen und Download starten"
      - wait for download, save to out_dir
      - write manifest record with the captured download URL (if available)
    """
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except ImportError:
        raise SystemExit(
            "Playwright not installed. Install with:\n"
            "  pip install playwright\n"
            "  playwright install chromium\n"
        )

    out_dir.mkdir(parents=True, exist_ok=True)

    # NOTE: We cannot guarantee selectors without inspecting live DOM.
    # This is the best-effort starting point.
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.goto(BASE_PAGE, wait_until="networkidle")

        # TODO: adjust these based on real UI controls.
        # Examples only:
        # page.fill("input[placeholder*='Adresse']", region)
        # page.keyboard.press("Enter")

        # If there is no search, you might need a coordinate click on the map.
        # That is site-specific; you would provide a municipality selection strategy.

        # Select dataset button (placeholder)
        # if dataset == "anzeigeraster":
        #     page.click("text=Anzeigedaten Globalstrahlung")
        # else:
        #     page.click("text=Abfragedaten Globalstrahlung")

        # Trigger download (placeholder)
        # with page.expect_download() as dl_info:
        #     page.click("text=ZIP-Archiv erstellen und Download starten")
        # download = dl_info.value

        browser.close()

    raise SystemExit(
        "Playwright mode is a scaffold and needs DOM selectors / selection method "
        "for municipality/region. If you capture the actual ZIP URL from DevTools, "
        "direct mode will be robust and simpler."
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["direct", "playwright"], default="direct")
    ap.add_argument("--region", required=True, help="Region/municipality name")
    ap.add_argument(
        "--dataset",
        choices=["anzeigeraster", "abfrageraster"],
        required=True,
        help="Which raster dataset to download",
    )
    ap.add_argument(
        "--url-template",
        default="",
        help="Required for direct mode. Example: https://.../download?dataset={dataset}&region={region}",
    )
    ap.add_argument("--out-dir", default="./data/zips")
    ap.add_argument("--manifest", default="./data/manifest.jsonl")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    manifest = Path(args.manifest)

    if args.mode == "direct":
        if not args.url_template:
            raise SystemExit(
                "Direct mode needs --url-template. "
                "Once you provide/capture the real ZIP endpoint, set it here."
            )
        direct_mode(args.region, args.dataset, args.url_template, out_dir, manifest)
    else:
        playwright_mode_stub(args.region, args.dataset, out_dir, manifest)


if __name__ == "__main__":
    main()
