#!/usr/bin/python3

import json
import math
import sqlite3
from pathlib import Path

import geopandas as gpd
from shapely.geometry import box, Point


def create_geopackage(
    sqlite_path: str, task: str = "bbox", filter: float = None
) -> str:
    if task not in ["bbox", "center"]:
        print("Unknown task. Supported are 'bbox' or 'center'.")
        return

    # Build output file name
    string = ""
    if task == "center":
        string += "_center"
    if filter:
        string += f"_filter({filter})"
    sqlite_path = Path(sqlite_path)
    geopackage_path = sqlite_path.parent.joinpath(
        "gpkg", sqlite_path.stem + string + ".gpkg"
    )
    sqlite_path.parent.joinpath("gpkg").mkdir(parents=True, exist_ok=True)

    # Connect to SQLite database
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
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
            timeout,
            time_result_insert,
            bbox
        FROM datasets
        WHERE bbox is not NULL""",
    )
    results = cursor.fetchall()

    areas = []
    metadata = []
    geometry = []

    for dataset in results:
        dataset_dict = {
            "key": dataset[0],
            "content_provider": dataset[1],
            "created_date": dataset[2],
            "modified_date": dataset[3],
            "id": dataset[4],
            "doi": dataset[5],
            "url_api": dataset[6],
            "url_html": dataset[7],
            "title": dataset[8],
            "description": dataset[9],
            "keywords": dataset[10],
            "sum_size": dataset[11],
            "files_types": dataset[12],
            "files": dataset[13],
            "files_http_status_code": dataset[14],
            "geospatial_flag": dataset[15],
            "download_flag": dataset[16],
            "timeout": dataset[17],
            "time_result_insert": dataset[18],
            # "bbox": dataset[19],
        }

        if ".csv" in dataset_dict["files_types"]:
            # if ".csv" in dataset_dict["files_types"] or ".zip" in dataset_dict["files_types"]:
            # continue
            pass

        # Define the bounding box (xmin, ymin, xmax, ymax)
        bbox = json.loads(dataset[19])

        # Calculate area
        # ! not a good way to calculate the are
        x_length = bbox[2] - bbox[0]
        y_length = bbox[3] - bbox[1]
        area_deg2 = x_length * y_length

        # Better use something like
        # https://gis.stackexchange.com/a/413350

        area_m2 = calculate_area(box(bbox[0], bbox[1], bbox[2], bbox[3]))

        dataset_dict["area_deg2"] = int(area_deg2)
        dataset_dict["area_m2"] = int(area_m2)
        dataset_dict["area_km2"] = area_m2 / 1000000

        areas.append(int(area_m2))

        # Filter faulty bboxes
        if filter and any(abs(x) < filter for x in bbox):
            # ! TODO: hier ist ein Bug. Es werden zu viele gefiltert. Testen, dass wirklich, und nur um die Null-Insel gefiltert wird.
            continue

        match task:
            case "bbox":
                geometry_to_insert = box(bbox[0], bbox[1], bbox[2], bbox[3])

            case "center":
                geometry_to_insert = Point(
                    (bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2
                )

        metadata.append(dataset_dict)
        geometry.append(geometry_to_insert)

    if len(areas) == 0:
        print("no bboxes in data")
        return

    max_area = max(areas)
    for index, dataset_dict in enumerate(metadata):
        area_m2_linear = dataset_dict["area_m2"] / max_area

        metadata[index]["area_m2__x"] = area_m2_linear
        metadata[index]["area_m2__x^2"] = area_m2_linear**2
        # metadata[index]["area_m2_sqrt"] = round(math.sqrt(area_m2_linear), 4)
        metadata[index]["area_m2__1-x"] = 1 - area_m2_linear
        metadata[index]["area_m2__1-x^2"] = 1 - area_m2_linear**2
        metadata[index]["area_m2__(1-x)^2"] = (1 - area_m2_linear) ** 2
        metadata[index]["area_m2__1/(50x)"] = (
            1 / (50 * area_m2_linear) if area_m2_linear > 0 else math.inf
        )
        metadata[index]["area_m2__1/(100x)"] = (
            1 / (100 * area_m2_linear) if area_m2_linear > 0 else math.inf
        )
        metadata[index]["area_m2__1/(200x)"] = (
            1 / (200 * area_m2_linear) if area_m2_linear > 0 else math.inf
        )

    layer_name = "bounding_box"

    # Create GeoDataFrame with all collected metadata and geometries
    gdf = gpd.GeoDataFrame(metadata, geometry=geometry, crs=4326)

    # Save the GeoDataFrame to a GeoPackage file
    if geopackage_path.exists():
        print(f"Output file {geopackage_path.name} already exists.")
        return geopackage_path
    else:
        gdf.to_file(geopackage_path, layer=layer_name, driver="GPKG")
    print(f"Saved {geopackage_path.name}.")

    return geopackage_path


def calculate_area(bbox: box) -> int:
    gdf_box = gpd.GeoDataFrame({"geometry": [bbox]}, crs="EPSG:4326")

    gdf_utm_box = gdf_box.to_crs("ESRI:53017")

    # https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.area.html
    area_m2 = int(gdf_utm_box.geometry.area.iloc[0])

    return area_m2


def count_bboxes(geopackage_with_bbox: dict, geopackage_to_count: dict):
    # For each GeoPackage with Bounding Boxes
    for bbox_key, bbox_layer in geopackage_with_bbox.items():
        # For each GeoPackage that needs to be counted
        for count_key, count_layer in geopackage_to_count.items():
            save_path = Path(bbox_key).with_name(
                f"{Path(bbox_key).stem}__countin__{Path(count_key).stem}.gpkg"
            )
            if save_path.exists():
                print(f"Output file {save_path} already exists.")
                continue

            # Load the GeoDataFrames from the GeoPackages
            gdf_bbox = gpd.read_file(bbox_key, layer=bbox_layer)
            gdf_count = gpd.read_file(count_key, layer=count_layer)

            # ! TODO same?
            gdf_bbox = gdf_bbox.to_crs(gdf_count.crs)

            # Create a spatial index for efficiency
            # https://geopandas.org/en/stable/docs/reference/sindex.html
            spatial_index = gdf_bbox.sindex

            def count_overlaps(geometry):
                # Find possible matches based on the bounds of the geometry
                possible_matches_index = list(
                    spatial_index.intersection(geometry.bounds)
                )
                possible_matches = gdf_bbox.iloc[possible_matches_index]
                # Precise test for overlaps, return sum
                return possible_matches.intersects(geometry).sum()

            # Add a new column with the count of overlaps
            gdf_count["overlap_count"] = gdf_count.geometry.apply(count_overlaps)

            # Save the updated GeoDataFrame to a new GeoPackage
            # ! TODO redo
            gdf_count.to_file(save_path, layer=count_layer, driver="GPKG")
            print(f"Saved {save_path}.")

    return
