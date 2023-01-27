#!/usr/bin/python
import argparse
import functools
import json
import re
import shutil
from pathlib import Path
from typing import Any, Iterator, cast

import polars as pl

########
# main #
########


def main() -> None:
    args = read_args()

    data = read_data(
        args.related_subjects, args.grades_lower, args.grades_higher
    )
    done_print("Read data.")
    load_print("Adding possible documents...")
    data = add_possible_documents(data)
    done_print("Added possible documents.")

    load_print("Writing data...")
    path = write_data(data)
    done_print(
        f"Wrote data to {path.resolve().relative_to(Path('.').resolve())}."
    )
    done_print("Data creation complete.")


def read_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--related-subjects",
        default="",
        help="Path of the related subjects json file"
        + " (see data/related_subjects.json)."
        + " Leave empty to not use any related subjects.",
    )
    parser.add_argument(
        "--grades-lower",
        type=int,
        default=0,
        help="Number of grades lower than the minimum document grade for"
        + " which to consider it possible.",
    )
    parser.add_argument(
        "--grades-higher",
        type=int,
        default=0,
        help="Number of grades higher than the maximum document grade for"
        + " which to consider it possible.",
    )
    return parser.parse_args()


def read_data(
    related_subjects_file: str, grades_lower: int, grades_higher: int
) -> pl.DataFrame:
    path = Path(__file__).parent / ".." / "data"
    load_print("Reading questions...")
    questions = read_questions(path)
    done_print("Read questions.")
    load_print("Reading pages...")
    pages = read_pages(
        path, related_subjects_file, grades_lower, grades_higher
    )
    done_print("Read pages.")
    load_print("Combining pages and questions...")
    return combine_documents(questions, pages)


def combine_documents(
    questions: pl.DataFrame, pages: pl.DataFrame
) -> pl.DataFrame:
    # create hash of urls to quickly associate links to ids
    page_hashes = {
        hash(url): id_ for url, id_ in zip(pages["url"], pages["id"])
    }

    questions = (
        questions.with_columns(
            [
                # convert all page links to the page ids, making sure each
                # page can only appear once in the list
                pl.col("page_links").apply(
                    lambda list_: list(
                        {
                            id_
                            for url in list_
                            if (
                                id_ := page_hashes.get(
                                    hash(
                                        url.replace(
                                            "https://www.alloprof.qc.ca", ""
                                        )
                                    )
                                )
                            )
                            is not None
                        }
                    )
                ),
                # convert question links to its id
                pl.col("question_links").apply(
                    lambda list_: list(
                        {
                            id_
                            for x in list_
                            if (
                                id_ := x.replace(
                                    "https://www.alloprof.qc.ca/zonedentraide/discussion/",  # noqa
                                    "",
                                ).split("/")[0]
                            )
                            != "https:"
                        }
                    )
                ),
            ]
        )
        # combine page and question links in a single list of ids
        .with_columns(
            pl.col("page_links").arr.concat("question_links").alias("links")
        )
        .drop(["page_links", "question_links"])
        .with_columns(
            [
                # to identify which document is a question or a page
                pl.lit(True).alias("is_query"),
                # to be able to find the actual question on the website
                pl.col("id")
                .apply(
                    lambda x: f"https://www.alloprof.qc.ca/zonedentraide/discussion/{x}"  # noqa
                )
                .alias("url"),
            ]
        )
    )

    pages = (
        pages
        # add columns needed to concatenate to questions
        .with_columns(
            [
                pl.col("id")
                .apply(lambda _: [])
                .cast(pl.List(pl.Utf8))
                .alias("links"),
                pl.lit(False).alias("is_query"),
                pl.col("id")
                .apply(lambda _: [])
                .cast(pl.List(pl.Utf8))
                .alias("images"),
                pl.col("url").apply(
                    lambda x: f"https://www.alloprof.qc.ca{x}"
                ),
            ]
        )
        # have pages repeated for each grade and subject it's relevant for
        .explode("grade").explode("subject")
    )

    return (
        pl.concat([questions, pages], how="diagonal")
        .rename({"links": "relevant"})
        .with_columns(pl.col("relevant").apply(sorted))
    )


def add_possible_documents(data: pl.DataFrame) -> pl.DataFrame:
    # extract list of possible documents for each combination of subject,
    # grade and language
    data = data.with_columns(
        (
            pl.col("subject")
            + ","
            + pl.col("grade")
            + ","
            + pl.col("language")
        ).alias("categories")
    )
    possible = (
        data.select(["id", "categories"])
        .unique()
        .groupby("categories")
        .agg(pl.list("id"))
        .rename({"id": "possible"})
    )
    # add possible documents only to questions
    data = pl.concat(
        [
            data.filter(pl.col("is_query"))
            .join(possible, on="categories")
            .drop("categories"),
            data.filter(~pl.col("is_query")).with_columns(
                pl.col("id")
                .apply(lambda _: [])
                .cast(pl.List(pl.Utf8))
                .alias("possible")
            ),
        ],
        how="diagonal",
    )
    # concatenate all subjects and grades for each document so there is only
    # a single unique document per line
    return (
        # combine all grades for each id and subject
        data.groupby(["id", "subject"])
        .agg([pl.exclude("grade").first(), pl.list("grade")])
        # combine all subjects for each id
        .groupby("id")
        .agg([pl.exclude("subject").first(), pl.list("subject")])
        # remove duplicate grades
        .with_columns(pl.col("grade").arr.unique())
    )


def write_data(data: pl.DataFrame) -> Path:
    path = Path(__file__).parent / ".." / "data" / "alloprof.csv"
    data = data.with_columns(
        [
            pl.col("subject").arr.join(";"),
            pl.col("grade").arr.join(";"),
            pl.col("images").arr.join(";"),
            pl.col("relevant").arr.join(";"),
            pl.col("possible").arr.join(";"),
        ]
    )
    data.write_csv(path)
    return path


#############
# questions #
#############


def read_questions(path: Path) -> pl.DataFrame:
    path = path / "questions"
    questions = read_questions_(path)
    answers = read_answers(path)
    grades = read_grades(path)
    subjects = read_subjects(path)

    return (
        questions
        # convert subject and grade ids to their name
        .join(subjects, on="CategoryID")
        .drop("CategoryID")
        .join(grades, on="GradeID")
        .drop("GradeID")
        # add answers and extract links and images
        .join(answers, on="id", how="left")
        .pipe(extract_relevant_links)
        .with_columns(
            [
                pl.col("id").cast(pl.Utf8),  # to make it consistent with pages
                pl.col("text").apply(extract_text_from_json),
                pl.col("text").apply(extract_images_from_json).alias("images"),
            ]
        )
    )


def read_questions_(path: Path) -> pl.DataFrame:
    return pl.read_json(path / "discussions.json").select(
        [
            pl.col("DiscussionID").alias("id"),
            pl.col("Body").alias("text"),
            pl.col("Language").alias("language"),
            pl.col("InsertUserID").alias("user"),
            pl.col("CategoryID"),
            pl.col("GradeID"),
        ]
    )


def read_answers(path: Path) -> pl.DataFrame:
    return (
        pl.read_json(path / "comments.json")
        .filter(~pl.col("DateAccepted").is_null())
        .select(
            [
                pl.col("DiscussionID").alias("id"),
                pl.col("Body").alias("answer"),
            ]
        )
    )


def read_grades(path: Path) -> pl.DataFrame:
    return pl.read_json(path / "grades.json").select(
        [pl.col("GradeID"), pl.col("Name").alias("grade")]
    )


def read_subjects(path: Path) -> pl.DataFrame:

    return pl.read_json(path / "categories.json").select(
        [
            pl.col("CategoryID"),
            # convert french subjects to english to make them consistent with
            # pages
            pl.col("Name").apply(convert_subject).alias("subject"),
        ]
    )


def extract_relevant_links(data: pl.DataFrame) -> pl.DataFrame:
    def extract_links(text: str) -> list[str]:
        return list(
            set(
                re.findall(
                    r"(https?:(?:\\)?/(?:\\)?/[a-zA-Z0-9/\\\.-]+)",
                    text.replace("\\/", "/"),
                )
            )
        )

    def extract_page_links(links: list[str]) -> list[str]:
        return [link for link in links if "/eleves/bv/" in link]

    def extract_question_links(links: list[str]) -> list[str]:
        return [link for link in links if "/zonedentraide/discussion" in link]

    return (
        data.with_columns(pl.col("answer").fill_null(""))
        .with_columns(pl.col("answer").apply(extract_links).alias("links"))
        .with_columns(
            [
                pl.col("links").apply(extract_page_links).alias("page_links"),
                pl.col("links")
                .apply(extract_question_links)
                .alias("question_links"),
            ]
        )
    )


def extract_text_from_json(json_: str) -> str:

    try:
        return " ".join(list(extract_text(json.loads(json_))))
    except json.JSONDecodeError:
        return ""


def extract_text(raw_section: list[dict] | dict) -> Iterator[str]:
    if isinstance(raw_section, list):
        for section_content in raw_section:
            yield from extract_text(section_content)

    elif isinstance(raw_section, dict):
        for section_tag, section_content in raw_section.items():
            if section_tag == "insert" and isinstance(section_content, str):
                yield re.sub(r"\s+", " ", section_content.strip())
            elif section_tag == "url":
                yield section_content.strip()  # type: ignore
            else:
                yield from extract_text(section_content)


def extract_images_from_json(json_: str) -> list[str]:

    try:
        return list(extract_images(json.loads(json_)))
    except json.JSONDecodeError:
        return []


def extract_images(raw_section: list[dict] | dict) -> Iterator[str]:
    if isinstance(raw_section, list):
        for section_content in raw_section:
            yield from extract_images(section_content)

    elif isinstance(raw_section, dict):
        for section_tag, section_content in raw_section.items():
            if section_tag == "url":
                yield cast(str, section_content)
            else:
                yield from extract_images(section_content)


#########
# pages #
#########


def read_pages(
    path: Path,
    related_subjects_file: str,
    grades_lower: int,
    grades_higher: int,
) -> pl.DataFrame:
    grades = read_grades(path / "questions")
    fr_pages = pl.read_json(path / "pages" / "page-content-fr.json")["data"]
    en_pages = pl.read_json(path / "pages" / "page-content-en.json")["data"]
    return (
        pl.DataFrame(
            [parse_page_data(page) for page in [*fr_pages, *en_pages]]
        )
        .with_columns(
            pl.col("subject")
            .apply(convert_subject)
            .apply(lambda subject: [subject])
        )
        .filter(pl.col("url") != "")
        .pipe(
            functools.partial(
                convert_grades,
                grades=grades,
                grades_lower=grades_lower,
                grades_higher=grades_higher,
            )
        )
        .pipe(
            functools.partial(
                add_related_subjects,
                related_subjects_file=related_subjects_file,
            )
        )
        .pipe(extract_page_text)
    )


def parse_page_data(data: dict[str, Any]) -> dict[str, str | int | list[str]]:
    try:
        page = {
            "id": data["file"]["uuid"],
            "url": data["file"]["breadcrumbs"]["current"]["routerLink"],
            "language": data["file"]["lang"],
            "subject": data["file"]["topic"],
            "grade": data["file"]["levels"],
            "title": data["file"]["title"],
            "tags": data["file"]["tags"],
            "content": " ".join(
                d["attributes"]["content"]
                for d in data["file"]["metatags"]
                if d["attributes"]["content"]
            ),
        }
        return {**page, "id": f"{page['id']}-{page['language']}"}
    except TypeError:
        return {}


def convert_grades(
    data: pl.DataFrame,
    grades: pl.DataFrame,
    grades_lower: int,
    grades_higher: int,
) -> pl.DataFrame:
    return (
        # add grades lower and higher
        data.with_columns(
            pl.col("grade").apply(
                lambda grades_: (
                    list(
                        range(
                            max(min(grades_) - grades_lower, 1),
                            min(grades_),
                        )
                    )
                    + list(grades_)
                    + list(
                        range(
                            max(grades_) + 1,
                            min(max(grades_) + grades_higher, 12) + 1,
                        )
                    )
                )
                if grades_ is not None
                else []
            )
        )
        # convert grades to their name
        .with_columns(
            pl.col("grade").apply(
                lambda grades_: pl.DataFrame({"GradeID": grades_})
                .join(grades, on="GradeID", how="left")["grade"]
                .to_list()
            )
        )
    )


def add_related_subjects(
    data: pl.DataFrame, related_subjects_file: str
) -> pl.DataFrame:
    if related_subjects_file == "":
        return data
    else:
        with open(related_subjects_file) as f:
            related_subjects = json.load(f)
        return data.with_columns(
            pl.col("subject").apply(
                lambda subject: list(subject) + related_subjects[subject[0]]
            )
        )


def extract_page_text(data: pl.DataFrame) -> pl.DataFrame:
    return data.with_columns(
        (
            pl.col("title")
            + " "
            + pl.col("tags").arr.join(" ")
            + " "
            + pl.col("content")
        ).alias("text")
    ).drop(["title", "tags", "content"])


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


def convert_subject(subject: str) -> str:
    subject_conversions = {
        "chemistry": ["chimie"],
        "contemporary_world": ["monde contemporain", "contemporary world"],
        "english": ["anglais"],
        "financial_ed": ["éducation financière", "financial education"],
        "french": ["français"],
        "geography": ["géographie"],
        "history": ["histoire"],
        "math": ["mathématiques", "mathematics"],
        "other": ["autre"],
        "physics": ["physique"],
        "science": ["sciences"],
    }
    match = [
        key
        for key, val in subject_conversions.items()
        if subject.lower() in [key, *val]
    ]
    if match:
        return match[0]
    else:
        return "other"


if __name__ == "__main__":
    main()
