#!/usr/bin/python
import json
import shutil
from pathlib import Path
from typing import Any, Iterable

import httpx
import polars as pl
import tqdm

########
# main #
########


def main() -> None:
    path = Path(__file__).parent / ".." / "data"
    load_print("Reading data...")
    data = read_data(path)
    urls = extract_image_urls(data)
    done_print("Read data.")
    load_print("Downloading images...")
    urls = download_images(urls, path)
    with open(path / "images" / "urls.json", "w") as f:
        json.dump(urls, f, indent=2)
    done_print("Downloaded images.")


def read_data(path: Path) -> pl.DataFrame:
    return pl.read_csv(path / "alloprof.csv").with_columns(
        [
            pl.col("subject").str.split(";"),
            pl.col("grade").str.split(";"),
            pl.col("images").str.split(";"),
            pl.col("relevant").str.split(";"),
            pl.col("possible").str.split(";"),
        ]
    )


def extract_image_urls(data: pl.DataFrame) -> dict[str, int]:
    return {
        url: i
        for i, url in enumerate(
            set().union(*(set(row) for row in data["images"]))
        )
    }


def download_images(urls: dict[str, int], path: Path) -> dict[str, int]:
    path = path / "images"
    path.mkdir(exist_ok=True)
    missing: list[str] = []
    for url, id_ in load_progress(urls.items(), "Downloading images..."):
        extension = url.split(".")[-1]
        if extension in ("jpg", "jpeg", "png"):
            with httpx.stream("GET", url) as resp:
                if resp.status_code == 200:
                    with open(path / f"{id_}.{extension}", "wb") as f:
                        for chunk in resp.iter_bytes():
                            if chunk:
                                f.write(chunk)
                else:
                    missing = [*missing, url]
        else:
            missing = [*missing, url]
    return {url: id_ for url, id_ in urls.items() if url not in missing}


#########
# utils #
#########


def load_print(text: str, symbol: str = "*") -> None:
    symbol = f"\033[1m[{symbol}]\033[0m"
    print(
        f"\r{symbol} {text}".ljust(shutil.get_terminal_size().columns),
        end="\r",
    )


def done_print(text: str, symbol: str = "+") -> None:
    symbol = f"\033[1m\033[92m[{symbol}]\033[0m"
    print(f"\r{symbol} {text}".ljust(shutil.get_terminal_size().columns))


def load_progress(
    iter_: Iterable[Any],
    text: str,
    symbol: str = "*",
    *args: Any,
    **kwargs: Any,
) -> Iterable[Any]:
    symbol = f"\033[1m[{symbol}]\033[0m"
    return tqdm.tqdm(
        iter_,
        f"\r{symbol} {text}",
        *args,
        leave=False,
        **kwargs,
    )


if __name__ == "__main__":
    main()
