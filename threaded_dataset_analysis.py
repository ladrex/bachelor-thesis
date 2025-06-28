#!/usr/bin/python3

import json
import logging
import math
import multiprocessing
import queue
import sqlite3
import tempfile
import threading
import time
import urllib.parse
from pathlib import Path
from requests import Session, HTTPError

import geoextent.lib.extent as geoextent
import geoextent.lib.extent as geoextent_help  # noqa: F401
from geoextent.__init__ import __version__ as geoextent_version  # noqa: F401


logging.basicConfig(level=logging.CRITICAL)
logging.getLogger("geoextent").setLevel(logging.CRITICAL)


class StopThreadException(Exception):
    """Exception raised if a thread needs to be terminated due to a stop event."""

    pass


def download_worker(
    stop_event: threading.Event,
    task_queue: queue.Queue,
    geoextent_queue: queue.Queue,
    result_queue: queue.Queue,
    worker_statistics: dict,
    content_provider: str,
    provider_sleep_info: dict,
):
    while not stop_event.is_set():
        try:
            key, doi, files, sum_size = task_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        with threading.Lock():
            worker_statistics["active_download_worker"][0] += 1

        session = Session()
        temp_parent = "/run/media/lars/8f0c1f09-2c90-4cb3-ac63-19295ea5ede3/tmp"
        Path(temp_parent).mkdir(parents=True, exist_ok=True)

        # print("DEBUG:", key, files, sum_size)

        dryad_sum_size_threshold_byte = 200000000
        # threshold determined by testing
        # SELECT key, doi, sum_size FROM datasets WHERE content_provider = "dryad" and sum_size < 209715200 ORDER BY sum_size DESC
        # no    208446870	doi:10.5061/dryad.kd51c5bcr https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.kd51c5bcr/download
        # no    200067489	doi:10.5061/dryad.83bk3j9s2 https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.83bk3j9s2/download
        # works 199666406	doi:10.5061/dryad.c3770vq   https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.c3770vq/download

        # https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory
        tmp_dir = tempfile.TemporaryDirectory(dir=temp_parent)

        # dryad: try to download single zip file
        if content_provider == "dryad" and sum_size < dryad_sum_size_threshold_byte:
            try:
                filename = "dataset.zip"
                file_link = (
                    "https://datadryad.org/api/v2/datasets/"
                    + urllib.parse.quote(doi, safe="")
                    + "/download"
                )
                resp = _request(
                    stop_event,
                    content_provider,
                    provider_sleep_info,
                    session,
                    file_link,
                    throttle=True,
                    stream=True,
                )
                filepath = Path(tmp_dir.name).joinpath(filename)
                # TODO: catch http error (?)
                with open(filepath, "wb") as dst:
                    for chunk in resp.iter_content(chunk_size=None):
                        dst.write(chunk)

                files_http_status = resp.status_code
                geoextent_queue.put(
                    [content_provider, key, sum_size, files_http_status, tmp_dir]
                )

                with threading.Lock():
                    worker_statistics["active_download_worker"][0] -= 1
                time.sleep(1)
                continue

            except ValueError as e:
                print("DEBUG: ValueError download:", e)
                metadata = {}
                files_http_status = "undefined"
                result_queue.put(
                    [content_provider, key, sum_size, files_http_status, metadata]
                )

                tmp_dir.cleanup()
                with threading.Lock():
                    worker_statistics["active_download_worker"][0] -= 1
                time.sleep(1)
                continue

            except HTTPError as e:
                if (
                    e.response.content
                    != b"The dataset is too large for zip file generation. Please download each file individually."
                ):
                    print("DEBUG: HTTPError download:", e)
                    metadata = {}
                    files_http_status = e.response.status_code
                    result_queue.put(
                        [content_provider, key, sum_size, files_http_status, metadata]
                    )

                    tmp_dir.cleanup()
                    with threading.Lock():
                        worker_statistics["active_download_worker"][0] -= 1
                    time.sleep(1)
                    continue
                else:
                    print(
                        "INFO: 'Dryad: The dataset is too large for zip file generation. Please download each file individually.'"
                    )
                    time.sleep(1)

            except StopThreadException:
                tmp_dir.cleanup()
                with threading.Lock():
                    worker_statistics["active_download_worker"][0] -= 1
                with threading.Lock():
                    worker_statistics["total_download_worker"][0] -= 1
                return

            except Exception as e:
                print(f"DEBUG: {key} Exception download:", e)
                files_http_status.append("undefined")

                tmp_dir.cleanup()
                with threading.Lock():
                    worker_statistics["active_download_worker"][0] -= 1
                time.sleep(1)
                continue

        # figshare, zenodo: download files (and dryad if single zip file failed)
        files_http_status = []
        for filename, file_link in files:
            try:
                resp = _request(
                    stop_event,
                    content_provider,
                    provider_sleep_info,
                    session,
                    file_link,
                    throttle=True,
                    stream=True,
                )
                filepath = Path(tmp_dir.name).joinpath(filename)
                # TODO: catch http error (?)
                with open(filepath, "wb") as dst:
                    for chunk in resp.iter_content(chunk_size=None):
                        dst.write(chunk)

                files_http_status.append(resp.status_code)

            except ValueError as e:
                print(f"DEBUG: ValueError download for {filename}:", e)
                files_http_status.append("undefined")

            except HTTPError as e:
                print(f"DEBUG: HTTPError download for {filename}:", e)
                files_http_status.append(e.response.status_code)

            except StopThreadException:
                tmp_dir.cleanup()
                with threading.Lock():
                    worker_statistics["active_download_worker"][0] -= 1
                with threading.Lock():
                    worker_statistics["total_download_worker"][0] -= 1
                return

            except Exception as e:
                print(f"DEBUG: {key} Exception download:", e)
                files_http_status.append("undefined")

            finally:
                time.sleep(1)

        geoextent_queue.put(
            [content_provider, key, sum_size, files_http_status, tmp_dir]
        )
        with threading.Lock():
            worker_statistics["active_download_worker"][0] -= 1

    with threading.Lock():
        worker_statistics["total_download_worker"][0] -= 1


def _request(
    stop_event: threading.Event,
    content_provider: str,
    provider_sleep_info: dict,
    session,
    url,
    throttle=False,
    **kwargs,
):
    while True:
        # TODO: except http error and retry
        try:
            response = session.get(url, **kwargs)
            response.raise_for_status()
            break  # break while loop
        except HTTPError as e:
            # handle different HTTP error codes for different providers
            # dryad     dict_keys(['undefined', '404', '502', '503'])
            # figshare  dict_keys(['404', '422'])
            # zenodo    dict_keys(['410', '502', '404', '504'])
            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status

            if e.response.status_code == 429:
                _throttle(stop_event, content_provider, provider_sleep_info, e.response)
            else:
                print(e.response.status_code)
                raise

    if throttle:
        _throttle(stop_event, content_provider, provider_sleep_info, response)

    return response


def _throttle(
    stop_event: threading.Event,
    content_provider: str,
    provider_sleep_info: dict,
    response,
):
    values = [
        (response.headers.get("x-ratelimit-remaining")),
        (response.headers.get("x-ratelimit-reset")),
        (response.headers.get("ratelimit-remaining")),
        (response.headers.get("ratelimit-reset")),
    ]
    http_error = response.status_code

    wait_seconds = 1
    reset_time = None

    match values:
        case [None, None, None, None]:
            if http_error == 429:
                wait_seconds = 60
            else:
                wait_seconds = 1

        case [_, _, None, None]:
            remaining = int(values[0])
            reset_time = int(values[1])

            if remaining < 2 or http_error == 429:
                wait_seconds = math.ceil(reset_time - time.time())

        case [None, None, _, _]:
            remaining = int(values[2])
            reset_time = int(values[3])

            if remaining < 2 or http_error == 429:
                wait_seconds = math.ceil(reset_time - time.time())

        case _:
            if http_error == 429:
                wait_seconds = 60
            else:
                wait_seconds = 1

    if wait_seconds > 60:
        print(f"INFO: Sleep {wait_seconds:.0f} s")
        provider_sleep_info[content_provider][0] = reset_time

    while True:
        if stop_event.is_set():
            raise StopThreadException
        if wait_seconds > 60:
            time.sleep(60)
            wait_seconds -= 60
        else:
            time.sleep(wait_seconds)
            break

    provider_sleep_info[content_provider][0] = None

    return


def geoextent_worker(
    geoextent_queue: queue.Queue,
    result_queue: queue.Queue,
    worker_statistics: dict,
    stop_event: list,
):
    while True:
        counter_stop_event = sum(event.is_set() for event in stop_event)
        if (
            counter_stop_event == len(stop_event)
            and geoextent_queue.empty()
            and worker_statistics["active_download_worker"][0] == 0
        ):
            break

        try:
            content_provider, key, sum_size, files_http_status, tmp_dir = (
                geoextent_queue.get(timeout=60)
            )
        except queue.Empty:
            continue

        with threading.Lock():
            worker_statistics["active_geoextent_worker"][0] += 1

        try:
            # multiprocesing to kill process
            # (for example, while a huge csv file is being processed,
            # the timeout mechanism of geoextent do not work)
            timeout_geoextent = 30 * 60  # [s]
            timeout_multiprocessing = 2 * timeout_geoextent  # [s]

            multiprocessing_queue = multiprocessing.Queue()
            process = multiprocessing.Process(
                target=run_geoextent,
                args=(
                    tmp_dir.name,
                    multiprocessing_queue,
                    timeout_geoextent,
                    key,
                ),
            )
            process.start()
            process.join(timeout=timeout_multiprocessing)
            if process.is_alive():
                print(
                    f"Process terminated after {timeout_multiprocessing} s. Key:", key
                )
                process.terminate()
                process.join()

            if not multiprocessing_queue.empty():
                metadata = multiprocessing_queue.get()
            else:
                metadata = {"timeout": timeout_multiprocessing}

        except Exception as e:
            print("DEBUG: Exception multiprocessing:", e)
            metadata = {}

        tmp_dir.cleanup()
        result_queue.put([content_provider, key, sum_size, files_http_status, metadata])

        with threading.Lock():
            worker_statistics["active_geoextent_worker"][0] -= 1

    with threading.Lock():
        worker_statistics["total_geoextent_worker"][0] -= 1


def run_geoextent(
    tmp: str, multiprocessing_queue: multiprocessing.Queue, timeout: int, key: int
) -> dict:
    try:
        metadata = geoextent.fromDirectory(path=tmp, bbox=True, timeout=timeout)
    except Exception as e:
        # TODO: mehr printen für debug
        print(f"DEBUG: {key} Exception geoextent:", e)
        metadata = {}
    finally:
        multiprocessing_queue.put(metadata)


def result_consumer(
    sqlite_path: str,
    stop_event: list,
    geoextent_queue: queue.Queue,
    result_queue: queue.Queue,
    statistics_dict: dict,
    time_begin: float,
    content_provider_list: list,
    worker_statistics: dict,
    provider_sleep_info: dict,
):
    threshold_time = 10 * 60 * 60  # [s]  (10 h)
    threshold_counter = 60  # datasets per content provider

    # Connect to SQLite database
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    while True:
        metadata_to_process = None
        try:
            content_provider, key, sum_size, files_http_status, metadata = (
                result_queue.get(timeout=10)
            )
            metadata_to_process = True
        except queue.Empty:
            pass

        if metadata_to_process:
            insert_query_statistics = """
                UPDATE statistics_dataset_analysis 
                SET processed_counter = ?,
                    processed_data_volume = ?,
                    timeout_counter = ?,
                    with_bbox = ?
                WHERE content_provider = ?
            """
            insert_query_datasets = """
                UPDATE datasets 
                SET files_http_status_code = ?,
                    bbox = ?,
                    processed_flag = 1,
                    timeout = ?,
                    time_result_insert = ?
                WHERE key = ?
            """

            statistics_dict[content_provider]["processed_counter"] += 1
            statistics_dict[content_provider]["processed_data_volume"] += sum_size

            bbox = metadata.get("bbox")
            timeout = int(timeout) if (timeout := metadata.get("timeout")) else None

            if bbox and not any(math.isnan(x) for x in bbox):
                statistics_dict[content_provider]["with_bbox"] += 1
                bbox_json = json.dumps(metadata["bbox"])
                files_http_status_code_json = json.dumps(files_http_status)
                cursor.execute(
                    insert_query_datasets,
                    (
                        files_http_status_code_json,
                        bbox_json,
                        timeout,
                        int(time.time()),
                        key,
                    ),
                )
            else:
                files_http_status_code_json = json.dumps(files_http_status)
                cursor.execute(
                    insert_query_datasets,
                    (files_http_status_code_json, None, timeout, int(time.time()), key),
                )

            if timeout:
                statistics_dict[content_provider]["timeout_counter"] += 1

            cursor.execute(
                insert_query_statistics,
                (
                    statistics_dict[content_provider]["processed_counter"],
                    statistics_dict[content_provider]["processed_data_volume"],
                    statistics_dict[content_provider]["timeout_counter"],
                    statistics_dict[content_provider]["with_bbox"],
                    content_provider,
                ),
            )

            conn.commit()

        print(
            "status:",
            "Runtime:",
            f"{time.strftime('%H:%M:%S', time.gmtime(time.time() - time_begin))} |",
            "Dryad:",
            generate_output_text(
                statistics_dict["dryad"], provider_sleep_info["dryad"][0]
            ),
            "Figshare:",
            generate_output_text(
                statistics_dict["figshare"], provider_sleep_info["figshare"][0]
            ),
            "Zenodo:",
            generate_output_text(
                statistics_dict["zenodo"], provider_sleep_info["zenodo"][0]
            ),
            f"Active download worker: {worker_statistics['active_download_worker'][0]}/{worker_statistics['total_download_worker'][0]} |",
            f"Active geoextent worker: {worker_statistics['active_geoextent_worker'][0]}/{worker_statistics['total_geoextent_worker'][0]} |",
            f"Geoextent-Queue: {geoextent_queue.qsize()} |",
            f"Result-Queue: {result_queue.qsize()}",
        )

        for index, name in enumerate(content_provider_list):
            # Stop download worker if number of processed datasets is reached
            first_condition = (
                statistics_dict[name]["processed_counter"] >= threshold_counter
            )
            # Stop download worker if time passed
            first_condition = (time.time() - time_begin) > threshold_time
            if first_condition and not stop_event[index].is_set():
                stop_event[index].set()
                print(f"Send stop signal to {name.title()} download worker stopped.")

        # Finish if all workers are stopped
        counter_stop_event = sum(event.is_set() for event in stop_event)
        if (
            counter_stop_event == len(stop_event)
            and geoextent_queue.empty()
            and result_queue.empty()
            and worker_statistics["active_download_worker"][0] == 0
            and worker_statistics["active_geoextent_worker"][0] == 0
        ):
            break

    conn.close()


def generate_output_text(statistics: dict, reset_time: int):
    if statistics["processed_counter"] > 0:
        text = (
            f"{statistics['with_bbox']}/{statistics['processed_counter']} "
            + f"({round(statistics['with_bbox'] / statistics['processed_counter'] * 100, 2):.2f} %) |"
        )
        if reset_time:
            text = (
                text[:-1]
                + f"(Sleep: {time.strftime('%H:%M:%S', time.gmtime(reset_time - time.time()))}) |"
            )
    else:
        text = "### |"

    return text


def main(
    sqlite_path: str = None,
):
    time_begin = time.time()

    print(time_begin)

    content_provider = ["dryad", "figshare", "zenodo"]

    if not Path(sqlite_path).is_file():
        print("invalid sqlite_path")
        return

    # Connect to SQLite database
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    # Get statistics
    cursor.execute("SELECT * FROM statistics_dataset_analysis")
    statistics = cursor.fetchall()  # is a list

    if not statistics:
        # Initialize SQLite statistics
        cursor.execute("""
            INSERT INTO statistics_dataset_analysis 
            (content_provider, processed_counter, processed_data_volume, timeout_counter, with_bbox) 
            VALUES 
            ('dryad', 0, 0, 0, 0),
            ('figshare', 0, 0, 0, 0),
            ('zenodo', 0, 0, 0, 0)
        """)
        conn.commit()

    cursor.execute(
        "SELECT * FROM statistics_dataset_analysis",
    )
    results = cursor.fetchall()

    statistics_dict = {}
    for row in results:
        statistics_dict[row[1]] = {
            "processed_counter": row[2],
            "processed_data_volume": row[3],
            "timeout_counter": row[4],
            "with_bbox": row[5],
        }

    stop_event = []
    worker_statistics = {
        "active_download_worker": [0],
        "total_download_worker": [len(content_provider)],
        "active_geoextent_worker": [0],
        "total_geoextent_worker": [2 * len(content_provider)],  # maybe just 3 not 6
    }
    provider_sleep_info = {}

    for provider in content_provider:
        provider_sleep_info[provider] = [None]

    # Create queues for tasks and results
    download_queues = [queue.Queue() for _ in range(len(content_provider))]
    geoextent_queue = queue.Queue()
    result_queue = queue.Queue()

    # Start consumer thread
    consumer_thread = threading.Thread(
        target=result_consumer,
        args=(
            sqlite_path,
            stop_event,
            geoextent_queue,
            result_queue,
            statistics_dict,
            time_begin,
            content_provider,
            worker_statistics,
            provider_sleep_info,
        ),
    )
    consumer_thread.start()

    # Start download threads
    download_workers = []
    geoextent_workers = []

    # 0.95-quantile [B]
    theshold_size_byte = [
        9476390755.90001,  #  8.83 GiB
        2200229344.25,     #  2.05 GiB
        19707810956.6,     # 18.35 GiB
    ]

    # Get all datasets of interest for each content provider
    for index, provider in enumerate(content_provider):
        stop_event.append(threading.Event())

        cursor.execute(
            "SELECT key, doi, files, sum_size FROM datasets WHERE content_provider = ? AND download_flag = 1 AND processed_flag = 0 AND sum_size < ?",
            (provider, theshold_size_byte[index]),
        )
        results = cursor.fetchall()

        for key, doi, files, sum_size in results:
            download_queues[index].put((key, doi, json.loads(files), sum_size))

        # TODO: maybe add two download worker per content provider
        # for i in range(2):
        download_thread = threading.Thread(
            target=download_worker,
            args=(
                stop_event[index],
                download_queues[index],
                geoextent_queue,
                result_queue,
                worker_statistics,
                provider,
                provider_sleep_info,
            ),
        )
        download_thread.start()
        download_workers.append(download_thread)

    conn.close()

    for i in range(worker_statistics["total_geoextent_worker"][0]):
        geoextent_thread = threading.Thread(
            target=geoextent_worker,
            args=(
                geoextent_queue,
                result_queue,
                worker_statistics,
                stop_event,
            ),
        )
        geoextent_thread.start()
        geoextent_workers.append(geoextent_thread)

    # Wait for all workers to stop
    for t in download_workers:
        t.join()
    for t in geoextent_workers:
        t.join()
    consumer_thread.join()

    time_diff = time.time() - time_begin
    time_str = time.strftime("%H:%M:%S", time.gmtime(time_diff))

    print(f"\r\033[KFinished dataset analysis in {time_str}")

    print(time.time())


if __name__ == "__main__":
    sqlite_path = "/home/lars/FINAL_metadata_db.sqlite3"
    main(sqlite_path)
