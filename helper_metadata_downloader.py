#!/usr/bin/python3

import math
import requests
import time
import urllib.parse
from dateutil import parser
from pathlib import Path


def get_metadata(content_provider: str, identifier: str, access_token: dict = {}) -> dict | int | None:
    match content_provider:
        case "dryad":
            # https://datadryad.org/api
            # https://github.com/datadryad/dryad-app/blob/main/documentation/apis/api_accounts.md
            # https://datadryad.org/api/v2/docs/
            #   Anonymous users of the API are limited to 30 requests per minute,
            #   with a lower limit for downloads of data files.
            #   Authenticated users of the API are limited to 120 requests per minute.

            # * 1st API request: get information about the dataset
            #   example for valid requests
            #   https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.j1fd7
            #   https://datadryad.org/api/v2/datasets/26651
            #   https://datadryad.org/api/v2/datasets/doi%3A10.6076%2FD1JP49

            base_url = "https://datadryad.org"
            identifier_html = urllib.parse.quote(identifier, safe="")

            response = get_response(
                content_provider,
                base_url + "/api/v2/datasets/" + identifier_html,
                timeout=30,
            )
            if not isinstance(response, requests.models.Response):
                return response
            data = response.json()

            try:
                message = data["message"]
                # print(f"debug:  {message}")
                return None
            except Exception:
                pass

            latest_version = data["_links"]["stash:version"]["href"]

            # * 2nd API request: get files for latest version
            #   example for valid requests
            #   https://datadryad.org/api/v2/versions/26724/files

            response = get_response(
                content_provider, base_url + latest_version + "/files", timeout=30
            )
            if not isinstance(response, requests.models.Response):
                return response
            data_files = response.json()

            # add metadata from data_files to data
            data["files_count"] = data_files["count"]
            data["files_total"] = data_files["total"]
            data["files_embedded"] = data_files["_embedded"]

        case "figshare":
            # https://help.figshare.com/article/how-to-use-the-figshare-api#basic-coding
            # https://docs.figshare.com/#figshare_documentation_api_description_rate_limiting
            # > We do not have automatic rate limiting in place for API requests.

            base_url = "https://api.figshare.com/v2/articles/"

            response = get_response(content_provider, base_url + identifier, timeout=30)
            if not isinstance(response, requests.models.Response):
                return response
            data = response.json()

        case "zenodo":
            # https://developers.zenodo.org/#records
            # https://developers.zenodo.org/#rate-limiting

            base_url = "https://zenodo.org/api/records/"
            ACCESS_TOKEN = access_token.get("zenodo")

            if ACCESS_TOKEN:
                response = get_response(
                    content_provider,
                    base_url + identifier,
                    params={"access_token": ACCESS_TOKEN},
                    timeout=30,
                )
            else:
                response = get_response(
                    content_provider,
                    base_url + identifier,
                    timeout=30,
                )
            if not isinstance(response, requests.models.Response):
                return response
            data = response.json()

        case _:
            print(f"Unsupported content provider: {content_provider}")

            raise SystemExit

    return data


def get_response(
    content_provider: str, url: str, **kwargs
) -> None | int | requests.models.Response:
    retries_counter = 0
    while True:
        try:
            response = requests.get(
                url,
                **kwargs,
            )
            response.raise_for_status()  # Raises an error for bad responses

            throttle(content_provider, response)

            break
        except requests.exceptions.HTTPError as e:
            http_error = handle_http_error(e)
            if (
                http_error != 429 and not (500 <= http_error <= 599)
            ) or retries_counter > 5:
                return http_error
            throttle(content_provider, e.response)
        except requests.exceptions.ReadTimeout as e:
            print(f"\r\033[Kdebug: {content_provider} timeout\n{e}\n")
            throttle(content_provider)
        except requests.exceptions.SSLError:
            throttle(content_provider)
        except requests.exceptions.ConnectionError:
            throttle(content_provider)
        except Exception as e:
            print(f"\r\033[KUnhandled exception:\n{e}\n")
            throttle(content_provider)

        retries_counter += 1
        #if retries_counter > 5:
        #    return None

    return response


def throttle(content_provider: str, response: requests.models.Response | None = None):
    match content_provider:
        case "dryad":
            time.sleep(0.5)

        case "figshare":
            time.sleep(1)

        case "zenodo":
            if response is not None:
                try:
                    limit = int(response.headers.get("x-ratelimit-limit"))
                    remaining = int(response.headers.get("x-ratelimit-remaining"))
                    reset = int(response.headers.get("x-ratelimit-reset"))

                    # print("debug:  Limit:", limit)
                    # print("debug:  Remaining:", remaining)
                    # print("debug:  Reset:", reset)
                    # print(f"debug:  Requests: {remaining}/{limit} left")
                    # print("debug:  Time until reset:", reset - time.time())

                    time.sleep(0.5)

                    if response is not None and remaining < 2:
                        wait_seconds = math.ceil(reset - time.time())
                        # print(f"Wait {wait_seconds:.0f} s until reset...")
                        time.sleep(wait_seconds)

                except Exception:
                    time.sleep(2)


def handle_http_error(e: requests.exceptions.HTTPError) -> bool | int:
    # https://docs.figshare.com/#figshare_documentation_api_description_errors
    #   Successful responses are always 200 and failed ones are always 400, even for failed authorization.
    #   https://docs.figshare.com/#public_article
    # https://developers.zenodo.org/#http-status-codes
    #   returns also error 410, although not in the documentation
    # Dryad: No error code information can be found

    match e.response.status_code:
        case 400:
            # print("\r\033[K400 Client Error: Bad Request. Request failed.")
            pass
        case 401:
            # print("\r\033[K401 Client Error: Unauthorized. Request failed, due to an invalid access token.")
            pass
        case 403:
            # print("\r\033[K403 Client Error: Forbidden. Request failed, due to missing authorization.")
            pass
        case 404:
            # print("\r\033[K404 Client Error: Not Found. Request failed, due to the resource not being found.")
            pass
        case 410:
            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/410
            # print("\r\033[K410 Client Error: Not Found. Request failed, due to the resource is no longer available at the origin server.")
            pass
        case 429:
            try:
                wait_time = int(e.response.headers.get("retry-after"))
            except Exception:
                wait_time = 60
            print(f"\r\033[K{e}")
            print(
                f"429 Client Error: Too Many Requests. Request failed, due to an invalid access token. Wait {wait_time} s."
            )
            time.sleep(wait_time)
        case error_code if 500 <= error_code <= 599:
            try:
                wait_time = int(e.response.headers.get("retry-after"))
            except Exception:
                wait_time = 60
            print(f"\r\033[K{e}")
            print(f"{e.response.status_code} Server Error. Wait {wait_time} s.")
            time.sleep(wait_time)
        case _:
            print(f"\r\033[K{e.response.status_code} Error")

    return e.response.status_code


def get_normalized_metadata(content_provider: str, metadata: dict) -> dict | None:
    sum_size, files_types, files, geospatial_flag, download_flag = analyse_files(
        content_provider, metadata
    )

    match content_provider:
        case "dryad":
            normalized_metadata = {
                "content_provider": content_provider,
                "created_date": get_date(metadata.get("publicationDate")),
                "modified_date": get_date(metadata.get("lastModificationDate")),
                "id": metadata.get("id"),
                "doi": metadata.get("identifier"),
                "url_api": None,
                "url_html": metadata.get("sharingLink"),
                "title": metadata.get("title"),
                "description": metadata.get("abstract"),
                "keywords": metadata.get("keywords"),
                "sum_size": sum_size,
                "files_types": files_types,
                "files": files,
                "geospatial_flag": geospatial_flag,
                "download_flag": download_flag,
            }

        case "figshare":
            normalized_metadata = {
                "content_provider": content_provider,
                "created_date": get_date(metadata.get("created_date")),
                "modified_date": get_date(metadata.get("modified_date")),
                "id": metadata.get("id"),
                "doi": metadata.get("doi"),
                "url_api": metadata.get("url"),
                "url_html": metadata.get("figshare_url"),
                "title": metadata.get("title"),
                "description": metadata.get("description"),
                "keywords": metadata.get("tags"),
                "sum_size": sum_size,
                "files_types": files_types,
                "files": files,
                "geospatial_flag": geospatial_flag,
                "download_flag": download_flag,
            }

        case "zenodo":
            normalized_metadata = {
                "content_provider": content_provider,
                "created_date": get_date(metadata.get("created")),
                "modified_date": get_date(metadata.get("modified")),
                "id": metadata.get("id"),
                "doi": metadata.get("doi"),
                "url_api": metadata.get("links", {}).get("self"),
                "url_html": metadata.get("links", {}).get("self_html"),
                "title": metadata.get("title"),
                "description": metadata.get("metadata", {}).get("description"),
                "keywords": metadata.get("metadata", {}).get("keywords"),
                "sum_size": sum_size,
                "files_types": files_types,
                "files": files,
                "geospatial_flag": geospatial_flag,
                "download_flag": download_flag,
            }

        case _:
            print("Unsupported content provider")
            return None

    return normalized_metadata


def get_date(date: str) -> str | None:
    """
    Returns a YYYY-MM-DD string.

    input:  2025-05-02
            2025-05-02T12:31:38Z
            2025-05-02T12:31:38.783221+00:00
    return: 2025-05-02
            None if invalid input
    """

    try:
        parsed_date = parser.isoparse(date)
        return parsed_date.strftime("%Y-%m-%d")
    except TypeError:
        # to handle NoneType
        # print("Invalid date format")
        return None
    except ValueError:
        # print("Invalid date format")
        return None


def analyse_files(
    content_provider: str, metadata: dict
) -> tuple[int | None, list, bool, bool]:
    """
    Returns sum_size, files_types, download_flag (e.g. [123456, ["tif", "zip"], True])
    """
    # taken from https://github.com/ladrex/geoextent/blob/master/geoextent/__main__.py
    supported_geospatial_formats = [
        ".geojson",
        ".csv",
        ".geotiff",
        ".tif",
        ".tiff",
        ".shp",
        ".gpkg",
        ".gpx",
        ".gml",
        ".kml",
    ]

    # taken from https://pypi.org/project/patool/
    supported_archive_formats = [
        ".7z",
        ".cb7",
        ".ace",
        ".cba",
        ".adf",
        ".alz",
        ".ape",
        ".a",
        ".arc",
        ".arj",
        ".bz2",
        ".bz3",
        ".cab",
        ".chm",
        ".Z",
        ".cpio",
        ".deb",
        ".dms",
        ".flac",
        ".gz)",
        ".iso",
        ".lrz",
        ".lha",
        ".lzh",
        ".lz",
        ".lzma",
        ".lzo",
        ".rpm",
        ".rar",
        ".cbr",
        ".rz)",
        ".shn",
        ".tar",
        ".cbt",
        ".udf",
        ".xz)",
        ".zip",
        ".jar",
        ".cbz",
        ".zoo",
        ".zst",
    ]

    sum_size = 0
    files_types = []
    geospatial_flag = False
    download_flag = False
    files = []

    match content_provider:
        case "dryad":
            if "stash:files" in metadata["files_embedded"]:
                # possible case: Download for this version of the dataset is unavailable
                # https://datadryad.org/api/v2/versions/117977/files
                # https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.tx95x69vd

                for file in metadata["files_embedded"]["stash:files"]:
                    # test if: Download is available
                    link_download = file["_links"].get("stash:download")
                    if link_download is None:
                        continue

                    name = file["path"]
                    link = "https://datadryad.org" + link_download["href"]
                    sum_size += file["size"]
                    extension = Path(file["path"]).suffix.lower()
                    files_types.append(extension)
                    files.append([name, link])

                    if extension in supported_geospatial_formats:
                        geospatial_flag = True
                    if extension in (
                        supported_geospatial_formats + supported_archive_formats
                    ):
                        download_flag = True

        case "figshare":
            if "files" in metadata:
                for file in metadata["files"]:
                    name = file["name"]
                    link = file["download_url"]
                    sum_size += file["size"]
                    extension = Path(file["name"]).suffix.lower()
                    files_types.append(extension)
                    files.append([name, link])

                    if extension in supported_geospatial_formats:
                        geospatial_flag = True
                    if extension in (
                        supported_geospatial_formats + supported_archive_formats
                    ):
                        download_flag = True

        case "zenodo":
            if "files" in metadata:
                for file in metadata["files"]:
                    name = Path(file["links"]["self"]).parent.name
                    link = file["links"]["self"]
                    sum_size += file["size"]
                    extension = Path(file["key"]).suffix.lower()
                    files_types.append(extension)
                    files.append([name, link])

                    if extension in supported_geospatial_formats:
                        geospatial_flag = True
                    if extension in (
                        supported_geospatial_formats + supported_archive_formats
                    ):
                        download_flag = True

    return sum_size, files_types, files, geospatial_flag, download_flag
