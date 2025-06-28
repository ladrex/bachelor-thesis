#!/usr/bin/python3

import numpy as np
import sqlite3
import statistics


def get_datasets_from_db(
    sqlite_path: str, query: str, params: None | tuple = None
) -> list:
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    return cursor.fetchall()


def value_to_percentile(list_in, value):
    list_in = np.array(list_in)

    return np.mean(list_in <= value) * 100


def calculate_stats(sqlite_path: str):
    output_path = f"{sqlite_path}_statistics.csv"

    content_provider = ["dryad", "figshare", "zenodo"]

    datasets = {
        "Alle": get_datasets_from_db(
            sqlite_path, query="SELECT sum_size FROM datasets"
        ),
        "Alle mit Download-Flag": get_datasets_from_db(
            sqlite_path, query="SELECT sum_size FROM datasets WHERE download_flag = 1"
        ),
        "Alle mit Geospatial-Flag": get_datasets_from_db(
            sqlite_path, query="SELECT sum_size FROM datasets WHERE geospatial_flag = 1"
        ),
        "Alle mit Processed-Flag": get_datasets_from_db(
            sqlite_path, query="SELECT sum_size FROM datasets WHERE processed_flag = 1"
        ),
        "Alle mit BBox": get_datasets_from_db(
            sqlite_path, query="SELECT sum_size FROM datasets WHERE bbox IS NOT NULL"
        ),
    }

    for name in content_provider:
        datasets[f"{name.title()}"] = get_datasets_from_db(
            sqlite_path,
            query="SELECT sum_size FROM datasets WHERE content_provider = ?",
            params=(name,),
        )
        datasets[f"{name.title()} mit Download-Flag"] = get_datasets_from_db(
            sqlite_path,
            query="SELECT sum_size FROM datasets WHERE content_provider = ? AND download_flag = 1",
            params=(name,),
        )
        datasets[f"{name.title()} mit Geospatial-Flag"] = get_datasets_from_db(
            sqlite_path,
            query="SELECT sum_size FROM datasets WHERE content_provider = ? AND geospatial_flag = 1",
            params=(name,),
        )
        datasets[f"{name.title()} mit Processed-Flag"] = get_datasets_from_db(
            sqlite_path,
            query="SELECT sum_size FROM datasets WHERE content_provider = ? AND processed_flag = 1",
            params=(name,),
        )
        datasets[f"{name.title()} mit BBox"] = get_datasets_from_db(
            sqlite_path,
            query="SELECT sum_size FROM datasets WHERE content_provider = ? AND bbox IS NOT NULL",
            params=(name,),
        )

    with open(output_path, "w") as f:
        f.write(
            "title;"
            + "Anzahl;"
            + "Durchschnitt [B];"
            + "Durchschnitt [GiB];"
            + "Median [B];"
            + "Median [GiB];"
            + "Min [B];"
            + "Min [GiB];"
            + "Max [B];"
            + "Max [GiB];"
            + "0,90-Quantil [B];"
            + "0,90-Quantil [GiB];"
            + "0,95-Quantil [B];"
            + "0,95-Quantil [GiB];"
            + "0,98-Quantil [B];"
            + "0,98-Quantil [GiB];"
            + "0,99-Quantil [B];"
            + "0,99-Quantil [GiB];"
            + "0,999-Quantil [B];"
            + "0,999-Quantil [GiB];"
            + "\n"
        )

    for key, value in datasets.items():
        # Extracting the first element from each tuple
        sizes = [size[0] for size in value]

        if sizes:
            sum_sizes_mean = statistics.mean(sizes)
            sum_sizes_median = statistics.median(sizes)
            sum_sizes_min = min(sizes)
            sum_sizes_max = max(sizes)
            percentile_90 = np.percentile(sizes, 90)
            percentile_95 = np.percentile(sizes, 95)
            percentile_98 = np.percentile(sizes, 98)
            percentile_99 = np.percentile(sizes, 99)
            percentile_999 = np.percentile(sizes, 99.9)
        else:
            sum_sizes_mean = None
            sum_sizes_median = None
            sum_sizes_min = None
            sum_sizes_max = None
            percentile_90 = None
            percentile_95 = None
            percentile_98 = None
            percentile_99 = None
            percentile_999 = None
        """
        print(
            f"{key}\n"
            + f"Anzahl: {len(sizes)}\n"
            + f"Durchschnitt:   {sum_sizes_mean / 1024**3}\n"
            + f"Median:         {sum_sizes_median / 1024**3}\n"
            + f"Min:            {sum_sizes_min / 1024**3}\n"
            + f"Max:            {sum_sizes_max / 1024**3}\n"
            + f"0,90-Quantil:   {percentile_90 / 1024**3} ({value_to_percentile(sizes, percentile_90)})\n"
            + f"0,95-Quantil:   {percentile_95 / 1024**3} ({value_to_percentile(sizes, percentile_95)})\n"
            + f"0,98-Quantil:   {percentile_98 / 1024**3} ({value_to_percentile(sizes, percentile_98)})\n"
            + f"0,99-Quantil:   {percentile_99 / 1024**3} ({value_to_percentile(sizes, percentile_99)})\n"
            + f"0,999-Quantil:  {percentile_999 / 1024**3} ({value_to_percentile(sizes, percentile_999)})\n"
        )
        """
        with open(output_path, "a") as f:
            if sizes:
                f.write(
                    f"{key};"
                    + f"{len(sizes)};"
                    + f"{sum_sizes_mean};"
                    + f"{sum_sizes_mean / 1024**3};"
                    + f"{sum_sizes_median};"
                    + f"{sum_sizes_median / 1024**3};"
                    + f"{sum_sizes_min};"
                    + f"{sum_sizes_min / 1024**3};"
                    + f"{sum_sizes_max};"
                    + f"{sum_sizes_max / 1024**3};"
                    + f"{percentile_90};"
                    + f"{percentile_90 / 1024**3};"
                    + f"{percentile_95};"
                    + f"{percentile_95 / 1024**3};"
                    + f"{percentile_98};"
                    + f"{percentile_98 / 1024**3};"
                    + f"{percentile_99};"
                    + f"{percentile_99 / 1024**3};"
                    + f"{percentile_999};"
                    + f"{percentile_999 / 1024**3};"
                    + "\n"
                )
            else:
                f.write(
                    f"{key};"
                    + f"{len(sizes)};"
                    + 18 * ";"
                    + "\n"
                )

    print(f"Saved {output_path}.")
