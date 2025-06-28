#!/usr/bin/python3

import pickle
import queue
import threading
import time
from pathlib import Path
from tinydb import TinyDB

from helper_metadata_downloader import get_metadata
from helper_metadata_downloader import get_normalized_metadata


def worker_process(
    stop_event: threading.Event,
    task_queue: queue.Queue,
    result_queue: queue.Queue,
    content_provider: str,
    access_token: dict,
):
    while not stop_event.is_set():
        try:
            identifier = task_queue.get(timeout=1.0)

            metadata = get_metadata(content_provider, identifier, access_token)
            result_queue.put([content_provider, identifier, metadata])
        except queue.Empty:
            continue


def result_consumer(
    stop_event: threading.Event,
    result_queue: queue.Queue,
    status: dict | None,
    checkpoint_path: str,
    db_path: str,
    time_begin: float,
):
    db = TinyDB(db_path)

    if not isinstance(status, dict):
        status = {
            "dryad": {
                "counter_successful": 0,
                "counter_failed": 0,
                "datasets_successful": [],
                "datasets_failed": [],
                "http_error": {},
            },
            "figshare": {
                "counter_successful": 0,
                "counter_failed": 0,
                "datasets_successful": [],
                "datasets_failed": [],
                "http_error": {},
            },
            "zenodo": {
                "counter_successful": 0,
                "counter_failed": 0,
                "datasets_successful": [],
                "datasets_failed": [],
                "http_error": {},
            },
        }

    metadata_pending_insert = []

    while True:
        dryad_good = status["dryad"]["counter_successful"]
        dryad_total = dryad_good + status["dryad"]["counter_failed"]

        figshare_good = status["figshare"]["counter_successful"]
        figshare_total = figshare_good + status["figshare"]["counter_failed"]

        zenodo_good = status["zenodo"]["counter_successful"]
        zenodo_total = zenodo_good + status["zenodo"]["counter_failed"]

        # number of identifiers per content provider from which the metadata is retrieved
        threshold = 100000

        if (
            dryad_good >= threshold
            and figshare_good >= threshold
            and zenodo_good >= threshold
        ):
            # stop Worker-Threads
            stop_event.set()

            # break if program was just started and threshold is reached immediately
            if time.time() - time_begin < 10:
                break

        if len(metadata_pending_insert) > 1000 or (
            stop_event.is_set() and result_queue.empty()
        ):
            if len(metadata_pending_insert) > 0:
                print("\r\033[K", end="")
                print("Write to pickle, tinydb.")

                with open(checkpoint_path, "wb") as output_file:
                    pickle.dump(status, output_file)

                db.insert_multiple(metadata_pending_insert)
                metadata_pending_insert = []

            if stop_event.is_set():
                break

        try:
            content_provider, identifier, metadata = result_queue.get(timeout=30)
        except queue.Empty:
            continue

        if metadata is None or isinstance(metadata, int):
            if metadata is None:
                error = "undefined"
            else:
                error = str(metadata)

            if error not in status[content_provider]["http_error"]:
                status[content_provider]["http_error"][error] = []

            status[content_provider]["counter_failed"] += 1
            status[content_provider]["datasets_failed"].append(identifier)
            status[content_provider]["http_error"][error].append(identifier)
        else:
            status[content_provider]["counter_successful"] += 1
            status[content_provider]["datasets_successful"].append(identifier)

            normalized_metadata = get_normalized_metadata(content_provider, metadata)
            metadata["normalized_metadata"] = normalized_metadata

            metadata_pending_insert.append(metadata)

            # print("\r\033[K", content_provider, identifier)

        if dryad_total > 0 and figshare_total > 0 and zenodo_total > 0:
            print(
                "\r\033[K",
                "status:",
                f"Runtime: {time.strftime('%H:%M:%S', time.gmtime(time.time() - time_begin))} |",
                f"Dryad: {dryad_good}/{dryad_total} ({round(dryad_good / dryad_total * 100, 2):.2f} %) |",
                f"Figshare: {figshare_good}/{figshare_total} ({round(figshare_good / figshare_total * 100, 2):.2f} %) |",
                f"Zenodo: {zenodo_good}/{zenodo_total} ({round(zenodo_good / zenodo_total * 100, 2):.2f} %) |",
                f"Queue: {result_queue.qsize()} |",
                f"Pending inserts: {len(metadata_pending_insert)}",
                end="\r",
            )

    # stop Worker-Threads
    stop_event.set()

    print("")


def metadata_harvester(
    files: dict = None,
    checkpoint_path: str = None,
    db_path: str = None,
    access_token: dict = {},
):
    time_begin = time.time()

    if not files:
        print("not files given")
        return
    elif not checkpoint_path:
        print("not checkpoint_path given")
        return
    elif not db_path:
        print("not db_path given")
        return

    status = None

    if Path(checkpoint_path).is_file():
        with open(checkpoint_path, "rb") as f:
            status = pickle.load(f)

        content_provider = list(status.keys())

        processed_identifiers = []

        for provider in content_provider:
            total = (
                status[provider]["counter_successful"]
                + status[provider]["counter_failed"]
            )
            processed_identifiers.append(total)

    stop_event = threading.Event()
    number_of_workers = len(files)
    content_provider = []

    # create queues for tasks and results
    task_queues = [queue.Queue() for _ in range(number_of_workers)]
    result_queue = queue.Queue()

    # start Worker-Threads
    workers = []

    for index, (content_provider_name, pickle_file) in enumerate(files.items()):
        content_provider.append(content_provider_name)

        with open(pickle_file, "rb") as file:
            list_ = pickle.load(file)

            # skip already processed identifier
            if Path(checkpoint_path).is_file():
                already_processed = processed_identifiers[index]
                list_ = list_[already_processed:]

        # fill task queue
        for item in list_:
            task_queues[index].put(item)

        thread = threading.Thread(
            target=worker_process,
            args=(
                stop_event,
                task_queues[index],
                result_queue,
                # index,
                content_provider_name,
                access_token,
            ),
        )
        thread.start()
        workers.append(thread)

    # start consumer thread
    consumer_thread = threading.Thread(
        target=result_consumer,
        args=(
            stop_event,
            result_queue,
            status,
            checkpoint_path,
            db_path,
            time_begin,
        ),
    )
    consumer_thread.start()

    # wait for all workers
    for t in workers:
        t.join()

    consumer_thread.join()

    time_diff = time.time() - time_begin
    time_str = time.strftime("%H:%M:%S", time.gmtime(time_diff))

    # https://stackoverflow.com/a/71507538
    print(f"\r\033[KFinished metadata harvesting in {time_str}")


if __name__ == "__main__":
    files = {
        "dryad": "FINAL/1_versions_of_run_2/0_shuffled_with_seed_74292449775793935952472534943397/dataset.json_auszug_dryad.json_identifiers.pickle_shuffled.pickle",
        "figshare": "FINAL/1_versions_of_run_2/0_shuffled_with_seed_74292449775793935952472534943397/dataset.json_auszug_figshare.json_identifiers.pickle_shuffled.pickle",
        "zenodo": "FINAL/1_versions_of_run_2/0_shuffled_with_seed_74292449775793935952472534943397/dataset.json_auszug_zenodo.json_identifiers.pickle_shuffled.pickle",
    }
    checkpoint_path = "/home/lars/FINAL_checkpoint.pickle"
    db_path = "/home/lars/FINAL_metadata_db.json"
    access_token = {
        # "dryad": "",
        # "figshare": "",
        # "zenodo": "",
    }

    metadata_harvester(
        files,
        checkpoint_path,
        db_path,
        access_token,
    )
