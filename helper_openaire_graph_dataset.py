#!/usr/bin/python3

import json
import re
from pathlib import Path
from tqdm import tqdm


def sort_by_provider(
    file: str, total_lines: int, content_provider: list
) -> [list, dict]:
    file_exists = 0
    statistics = {}
    statistics["publishers"] = {}

    output_input_sorted = []

    for name in content_provider:
        statistics[name] = {"counter": 0, "provider": {}}
        output_file = f"{file}_auszug_{name}.json"
        output_input_sorted.append(output_file)

        if Path(output_file).is_file():
            file_exists += 1

    if file_exists == (len(content_provider)):
        print(f"File {Path(file).name} was already processed.\n")

        # TODO: also return the statistics somehow
        return output_input_sorted, None

    with open(file) as f:
        counter = 0
        for line in tqdm(f, total=total_lines, desc="Progress", unit="lines"):
            counter += 1

            dataset = json.loads(line)
            publisher = dataset.get("publisher")

            # check if publisher extraction was successful
            if publisher:
                if publisher not in statistics["publishers"]:
                    statistics["publishers"][publisher] = 1
                else:
                    statistics["publishers"][publisher] += 1
            else:
                continue

            for index, search_content_provider in enumerate(content_provider):
                if search_content_provider in str(publisher).lower():
                    statistics[search_content_provider]["counter"] += 1

                    if publisher not in statistics[search_content_provider]["provider"]:
                        statistics[search_content_provider]["provider"][publisher] = 1
                    else:
                        statistics[search_content_provider]["provider"][publisher] += 1

                    with open(output_input_sorted[index], "a") as out:
                        out.write(f"{line}")

    print(f"\nTotal lines processed: {counter}")

    print(f"Sorted {Path(file).name} by given content provider.\n")

    return output_input_sorted, statistics


def get_identifier(content_provider: str, sample_dataset: str) -> list:
    # list of extracted identifiers which is returned
    identifiers = []

    failed_counter = 0
    
    match content_provider:
        case "dryad":
            with open(sample_dataset) as f:
                for current_line_number, line in enumerate(f):
                    record_identifiers = []

                    record = json.loads(line)

                    instances = record["instances"]

                    id_counter = 0

                    for instance in instances:
                        if "pids" in instance:
                            for pid in instance["pids"]:
                                if pid["scheme"] in ["doi"]:
                                    record_identifiers.append(pid["value"])
                                    id_counter += 1
                        elif "alternateIdentifiers" in instance:
                            for aid in instance["alternateIdentifiers"]:
                                if aid["scheme"] in ["doi"]:
                                    record_identifiers.append(aid["value"])
                                    id_counter += 1

                    if not id_counter:
                        print(f"debug:  no doi in dataset {current_line_number + 1}")
                        failed_counter += 1
                        continue

                    result = []
                    counter_success = 0
                    pattern = r"(10\.5061/dryad\.[a-zA-Z0-9]+)(?:/\d+)?"

                    for identifier in record_identifiers:
                        match = re.search(pattern, identifier)
                        if match:
                            id = f"doi:{match.group(1)}"
                            if id not in result:
                                result.append(id)
                                counter_success += 1
                    if not counter_success:
                        print(
                            f"debug:  failed to get {content_provider} id from record {current_line_number + 1} : {record_identifiers}"
                        )
                        failed_counter += 1
                        continue

                    result.sort()
                    identifiers.append(result[0])

                    # dryad identifier:
                    #     10.5061/dryad.70d46/3
                    # doi:10.5061/dryad.70d46
                    #
                    #
                    # valid identifier:
                    # doi:10.6076/D1JP49

        case "figshare":
            with open(sample_dataset) as f:
                for current_line_number, line in enumerate(f):
                    record_identifiers = []

                    record = json.loads(line)

                    instances = record["instances"]

                    id_counter = 0

                    for instance in instances:
                        if "pids" in instance:
                            for pid in instance["pids"]:
                                if pid["scheme"] in ["doi"]:
                                    record_identifiers.append(pid["value"])
                                    id_counter += 1
                        elif "alternateIdentifiers" in instance:
                            for aid in instance["alternateIdentifiers"]:
                                if aid["scheme"] in ["doi"]:
                                    record_identifiers.append(aid["value"])
                                    id_counter += 1

                    if not id_counter:
                        print(f"debug:  no doi in dataset {current_line_number + 1}")
                        failed_counter += 1
                        continue

                    result = []
                    counter_success = 0
                    pattern = r"\.(\d+)(?:_d\d+)?(?:\.v\d+)?$"

                    for identifier in record_identifiers:
                        match = re.search(pattern, identifier)
                        if match:
                            id = match.group(1)
                            if id not in result and id.isdigit():
                                result.append(id)
                                counter_success += 1
                    if not counter_success:
                        print(
                            f"debug:  failed to get {content_provider} id from record {current_line_number + 1} : {record_identifiers}"
                        )
                        failed_counter += 1
                        continue

                    result.sort()
                    identifiers.append(result[0])

                    # figshare identifier:
                    # ['10.6084/m9.figshare.25903798.v1', '10.6084/m9.figshare.25903798']
                    # ['10.6084/m9.figshare.9978467.v1', '10.6084/m9.figshare.9978473', '10.6084/m9.figshare.9978473.v1']   # hier unterschiedliche ids, aber dieselben Dateien
                    # ['10.6084/m9.figshare.c.4372913', '10.6084/m9.figshare.c.4372913.v1', '10.6084/m9.figshare.c.4372913.v2']
                    # ['10.6084/m9.figshare.c.3636047_d10', '10.6084/m9.figshare.c.3636047_d10', '10.6084/m9.figshare.c.3636047_d10.v1', '10.6084/m9.figshare.c.3636047_d10.v1']
                    # 10.25384/sage.c.4409609
                    # 10.25387/g3.7586393

        case "zenodo":
            with open(sample_dataset) as f:
                for current_line_number, line in enumerate(f):
                    record_identifiers = []

                    record = json.loads(line)

                    instances = record["instances"]

                    id_counter = 0

                    for instance in instances:
                        if "pids" in instance:
                            for pid in instance["pids"]:
                                if pid["scheme"] in ["doi", "oai"]:
                                    record_identifiers.append(pid["value"])
                                    id_counter += 1
                        elif "alternateIdentifiers" in instance:
                            for aid in instance["alternateIdentifiers"]:
                                if aid["scheme"] in ["doi", "oai"]:
                                    record_identifiers.append(aid["value"])
                                    id_counter += 1

                    if not id_counter:
                        print(f"debug:  no doi in dataset {current_line_number + 1}")
                        failed_counter += 1
                        continue

                    result = []
                    counter_success = 0
                    pattern = r"(?:10\.\d+/zenodo\.)(\d+)(?:/\d+)?"

                    for identifier in record_identifiers:
                        match = re.search(pattern, identifier)
                        if match:
                            id = match.group(1)
                        elif "oai:zenodo.org:" in identifier:
                            id = str(identifier).replace("oai:zenodo.org:", "")
                        if id not in result and id.isdigit():
                            result.append(id)
                            counter_success += 1
                    if not counter_success:
                        print(
                            f"debug:  failed to get {content_provider} id from record {current_line_number + 1} : {record_identifiers}"
                        )
                        failed_counter += 1
                        continue

                    result.sort()
                    identifiers.append(result[0])

                    # zenodo identifier:
                    # 10.5281/zenodo.5310135
                    # oai:zenodo.org:1220711
                    # http://data.europa.eu/88u/dataset/oai-zenodo-org-6619395
                    # 10.12345/zenodo.12345
                    # 10.5282/zenodo.447779
                    # 10.1364/zenodo.496336
                    # 10.5081/zenodo.3634756

    print(f"debug:  failed id extractions: {failed_counter}")
    print(f"Succesfully extracted {len(identifiers)} identifier.")

    return identifiers


r"""
pattern = r'.(\d+)(?:.v\d+)?$'

r           Backslashes sind normale Zeichen
.           beliebiges Zeichen
(\d+)
    \d      Ziffer (0-9)
    +       eine oder mehrere Ziffern
    ()      wird als Gruppe erfasst, die später ausgelesen werden kann
(?:.v\d+)?
    (?:...) "non-capturing group", d.h. diese Gruppe wird nicht als eigene Gruppe gespeichert, sondern nur zum Prüfen des Musters verwendet
    \.v     sucht nach einem Punkt gefolgt von einem "v"
    \d+     eine oder mehrere Ziffern
    ?       nach der Klammer bedeutet, dass dieser gesamte Teil optional ist
$           steht für das Ende der Zeichenkette. Das Muster muss also am Ende des Strings stehen

mit
(?:_d\d+)?
pattern = r'.(\d+)(?:_d\d+)?(?:.v\d+)?$'


hier
pattern = r"(10\.5061/dryad\.[a-zA-Z0-9]+)(?:/\d+)?"

r                   Backslashes sind normale Zeichen
(10\.5061/dryad\.[a-zA-Z0-9]+)
    ()              wird als Gruppe erfasst, die später ausgelesen werden kann
    10              10
    \.              .
    5061/dryad      5061/dryad
    \.              .
    [a-zA-Z0-9]+    eine oder mehrere Buchstaben, Zahlen
(?:/\d+)?
    (?:...)         "non-capturing group", d.h. diese Gruppe wird nicht als eigene Gruppe gespeichert, sondern nur zum Prüfen des Musters verwendet
    /               /
    \d+             eine oder mehrere Ziffern
    ?               Gruppe ist optional
"""
