#!/usr/bin/python3

import csv
import json
import sqlite3
from pathlib import Path


def save_csv(sqlite_path: str, csv_path: str, query: str):
    if csv_path.exists():
        print("csv file exists.")
        return

    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    cursor.execute(query)
    results = cursor.fetchall()

    data = []
    data.append(
        [
            "key",
            "content_provider",
            "created_date",
            "modified_date",
            "id",
            "doi",
            "url_api",
            "url_html",
            "title",
            "description",
            "keywords",
            "sum_size",
            "files_types",
            "files",
            "files_http_status_code",
            "geospatial_flag",
            "download_flag",
            "processed_flag",
            "timeout",
            "time_result_insert",
            "bbox",
        ]
    )

    for dataset in results:
        bbox = dataset[20]

        if bbox:
            minx, miny, maxx, maxy = json.loads(bbox)
            wkt = f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"

            # verify with https://wktmap.com
        else:
            wkt = None

        insert = [
            dataset[0],
            dataset[1],
            dataset[2],
            dataset[3],
            dataset[4],
            dataset[5],
            dataset[6],
            dataset[7],
            dataset[8],
            dataset[9],
            dataset[10],
            dataset[11],
            dataset[12],
            dataset[13],
            dataset[14],
            dataset[15],
            dataset[16],
            dataset[17],
            dataset[18],
            dataset[19],
            wkt,
        ]
        data.append(insert)

    with open(csv_path, "w", newline="", encoding="utf-8") as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(data)
    
    print(f"Saved {csv_path}.")


def create_csv(sqlite_path: str):
    COLUMNS = """
        key,
        content_provider,
        created_date,
        modified_date,
        id,
        doi,
        url_api,
        url_html,
        title,
        description,
        keywords,
        sum_size,
        files_types,
        files,
        files_http_status_code,
        geospatial_flag,
        download_flag,
        processed_flag,
        timeout,
        time_result_insert,
        bbox
    """

    EXPORT_JOBS = [
        ("", ""),
        ("_processed", "WHERE processed_flag is 1"),
        ("_processed_with_bbox", "WHERE bbox is not NULL"),
    ]

    for label, where in EXPORT_JOBS:
        csv_path = Path(sqlite_path).with_name(Path(sqlite_path).stem + label).with_suffix(".csv")
        query = f"SELECT {COLUMNS} FROM datasets {where}"
        save_csv(sqlite_path, csv_path, query)


if __name__ == "__main__":
    sqlite_path = "/home/lars/Dokumente/Studium2/Bachelorarbeit_/data/FINAL_metadata_db_vacuum.sqlite3"

    create_csv(sqlite_path)
