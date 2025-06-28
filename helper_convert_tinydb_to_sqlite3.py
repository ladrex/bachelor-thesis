#!/usr/bin/python3

import sqlite3
import json
from pathlib import Path
from tinydb import TinyDB


def convert_tinydb_to_sqlite3(
    tinydb_paths: list,
    sqlite_path: str,
):
    if Path(sqlite_path).is_file():
        print(f"Output file {sqlite_path} already exist.")
        return

    all_datasets = []
    print("Reading tinydb...")
    for tinydb_path in tinydb_paths:
        db = TinyDB(tinydb_path)
        all_datasets.extend(db.all())

    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS statistics_dataset_analysis (
            id INTEGER PRIMARY KEY,
            content_provider TEXT,
            processed_counter INTEGER,
            processed_data_volume INTEGER,
            timeout_counter INTEGER,
            with_bbox INTEGER
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            key INTEGER PRIMARY KEY,
            content_provider TEXT,
            created_date TEXT,
            modified_date TEXT,
            id TEXT,
            doi TEXT,
            url_api TEXT,
            url_html TEXT,
            title TEXT,
            description TEXT,
            keywords TEXT,
            sum_size INTEGER,
            files_types TEXT,
            files TEXT,
            files_http_status_code TEXT,
            geospatial_flag INTEGER,
            download_flag INTEGER,
            processed_flag INTEGER,
            timeout INTEGER,
            bbox TEXT,
            time_result_insert INTEGER,
            metadata TEXT
        )
    """)

    # Use parameterized query for inserting data
    insert_query = """
        INSERT INTO datasets (
            content_provider, created_date, modified_date, id, doi, url_api, 
            url_html, title, description, keywords, sum_size, 
            files_types, files, files_http_status_code, geospatial_flag, download_flag, processed_flag, timeout, bbox, time_result_insert, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    counter = 0

    # Insert data from TinyDB into SQLite
    for dataset in all_datasets:
        files_http_status_code = None
        timeout = None
        bbox = None
        time_result_insert = None

        metadata = dataset
        normalized_metadata = metadata.pop("normalized_metadata")

        cursor.execute(
            insert_query,
            (
                normalized_metadata["content_provider"],
                normalized_metadata["created_date"],
                normalized_metadata["modified_date"],
                str(normalized_metadata["id"]),
                normalized_metadata["doi"],
                normalized_metadata["url_api"],
                normalized_metadata["url_html"],
                normalized_metadata["title"],
                normalized_metadata["description"],
                json.dumps(normalized_metadata["keywords"]),
                normalized_metadata["sum_size"],
                json.dumps(normalized_metadata["files_types"]),
                json.dumps(normalized_metadata["files"]),
                files_http_status_code,
                1 if normalized_metadata["geospatial_flag"] else 0,
                1 if normalized_metadata["download_flag"] else 0,
                0,
                timeout,
                bbox,
                time_result_insert,
                json.dumps(metadata),
            ),
        )

        counter += 1
        print(counter, end="\r")

    print("Finished writing sqlite.")

    # Save changes and close connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    tinydb_paths = [
        "/home/lars/FINAL_metadata_db.json",
        "/home/lars/FINAL_metadata_db_part2.json",
    ]
    sqlite_path = str(Path(tinydb_paths[0]).with_suffix(".sqlite3"))

    convert_tinydb_to_sqlite3(tinydb_paths, sqlite_path)
