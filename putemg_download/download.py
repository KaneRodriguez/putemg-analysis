import os
import sys
import urllib.request
import re
import typing
import asyncio

import pandas as pd
import aiohttp


BASE_URL = "https://chmura.put.poznan.pl/s/G285gnQVuCnfQAx/download?path=%2F"

VIDEO_1080p_DIR = "Video-1080p"
VIDEO_576p_DIR = "Video-576p"
DEPTH_DIR = "Depth"
DATA_HDF5_DIR = "Data-HDF5"
DATA_CSV_DIR = "Data-CSV"


def usage():
    print(
        "Usage: {:s} <experiment_type> <media_type> [<id1> <id2> ...]".format(
            os.path.basename(__file__)
        )
    )
    print()
    print("Arguments:")
    print(
        "    <experiment_type>    comma-separated list of experiment types (supported types: emg_gestures, emg_force)"
    )
    print(
        "    <media_type>         comma-separated list of media (supported types: data-csv, data-hdf5, depth, video-1080p, video-576p)"
    )
    print(
        "    [<id1> <id2> ...]    optional list of two-digit participant IDs, fetches all if none are given"
    )
    print()
    print("Examples:")
    print("{:s} emg_gestures data-hdf5,video-1080p".format(os.path.basename(__file__)))
    print(
        "{:s} emg_gestures,emg_force data-csv,depth 03 04 07".format(
            os.path.basename(__file__)
        )
    )


def parse_record(name):
    experiment_name_regexp = (
        r"^(?P<type>\w*)-(?P<id>\d{2})-(?P<trajectory>\w*)-"
        r"(?P<date>\d{4}-\d{2}-\d{2})-(?P<time>\d{2}-\d{2}-\d{2}-\d{3})"
    )
    tags = re.search(experiment_name_regexp, name)
    if not tags:
        raise Warning("Wrong record", name)
    return (
        tags.group("type"),
        tags.group("id"),
        tags.group("trajectory"),
        tags.group("date"),
        tags.group("time"),
    )


def download_progress(blocknum, blocksize, totalsize):
    readsofar = blocknum * blocksize
    if totalsize > 0:
        percent = readsofar * 1e2 / totalsize
        s = "\r%5.1f%% %*d / %d" % (percent, len(str(totalsize)), readsofar, totalsize)
        sys.stderr.write(s)
        if readsofar >= totalsize:  # near the end
            sys.stderr.write("\n")
    else:  # total size is unknown
        sys.stderr.write("read %d\n" % (readsofar,))


async def download(
    experiment_types: typing.List[str],
    media_types: typing.List[str],
    data_ids: typing.List[str] = None,
):
    data_ids = data_ids or []

    if not experiment_types or not media_types:
        usage()
        return

    for e in experiment_types:
        if not e in ("emg_gestures", "emg_force"):
            print('Invalid experiment type "{:s}"'.format(e))
            usage()
            return
    experiment_types = set(experiment_types)

    for m in media_types:
        if not any(
            m in t
            for t in ("data-csv", "data-hdf5", "depth", "video-1080p", "video-576p")
        ):
            print('Invalid media type "{:s}"'.format(m))
            usage()
            return
    media_types = set(media_types)

    print(experiment_types)
    print(media_types)

    records_available = (
        urllib.request.urlopen(BASE_URL + "&files=records.txt")
        .read()
        .decode("utf-8")
        .splitlines()
    )

    records = list()
    ids = set()
    for r in records_available:
        experiment_type, id, trajectory, date, time = parse_record(r)
        records.append((experiment_type, id, trajectory, date, time))
        ids.add(id)

    ids_requested = set()
    if data_ids:
        for id in data_ids:
            if not re.match(r"^[0-9]{2}$", id):
                print('Invalid id "{:s}"'.format(id))
                usage()
                return
            if not id in ids:
                print('ID "{:s}" not available'.format(id))
                return
            ids_requested.add(id)
        ids = ids.intersection(ids_requested)

    ids = list(ids)
    ids.sort()

    print(ids)

    if "data-csv" in media_types:
        os.makedirs(DATA_CSV_DIR, exist_ok=True)
    if "data-hdf5" in media_types:
        os.makedirs(DATA_HDF5_DIR, exist_ok=True)
    if "depth" in media_types:
        os.makedirs(DEPTH_DIR, exist_ok=True)
    if "video-1080p" in media_types:
        os.makedirs(VIDEO_1080p_DIR, exist_ok=True)
    if "video-576p" in media_types:
        os.makedirs(VIDEO_576p_DIR, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for r in records:
            if r[0] in experiment_types and r[1] in ids:
                record = "{:s}-{:s}-{:s}-{:s}-{:s}".format(r[0], r[1], r[2], r[3], r[4])
                if "data-csv" in media_types:
                    tasks.append(
                        asyncio.create_task(
                            fetch_data(session, BASE_URL, DATA_CSV_DIR, record, "zip")
                        )
                    )
                if "data-hdf5" in media_types:
                    tasks.append(
                        asyncio.create_task(
                            fetch_data(session, BASE_URL, DATA_HDF5_DIR, record, "hdf5")
                        )
                    )
                if "depth" in media_types and r[0] != "emg_force":
                    tasks.append(
                        asyncio.create_task(
                            fetch_data(session, BASE_URL, DEPTH_DIR, record, "zip")
                        )
                    )
                if "video-1080p" in media_types:
                    tasks.append(
                        asyncio.create_task(
                            fetch_data(
                                session, BASE_URL, VIDEO_1080p_DIR, record, "mp4"
                            )
                        )
                    )
                if "video-576p" in media_types:
                    tasks.append(
                        asyncio.create_task(
                            fetch_data(session, BASE_URL, VIDEO_576p_DIR, record, "mp4")
                        )
                    )

        await asyncio.gather(*tasks)


async def fetch_data(
    session, base_url, data_dir, record, file_type, overwrite_existing: bool = False
) -> None:
    url = base_url + data_dir + "&files=" + record + f".{file_type}"
    file_path = data_dir + "/" + record + f".{file_type}"

    if not overwrite_existing:
        if os.path.isfile(file_path):
            return

    print(f"Fetching {file_path}")

    async with session.get(url) as response:
        data = await response.read()
        print(f"Storing {file_path}")
        with open(file_path, "wb") as file:
            file.write(data)


def create_dataframe_from_records_meta(
    records_meta: typing.List[typing.Tuple[str, str, str, str, str]]
) -> pd.DataFrame:
    df = pd.DataFrame(
        records_meta, columns=["type", "id", "trajectory", "date", "time"]
    )
    return df


def get_records_dataframe(
    experiment_types: typing.List[str],
    media_types: typing.List[str],
    data_ids: typing.List[str] = None,
) -> pd.DataFrame:
    data_ids = data_ids or []
    records_meta = []

    if not experiment_types or not media_types:
        usage()
        return create_dataframe_from_records_meta(records_meta)

    for e in experiment_types:
        if not e in ("emg_gestures", "emg_force"):
            print('Invalid experiment type "{:s}"'.format(e))
            usage()
            return create_dataframe_from_records_meta(records_meta)

    experiment_types = set(experiment_types)

    for m in media_types:
        if not any(
            m in t
            for t in ("data-csv", "data-hdf5", "depth", "video-1080p", "video-576p")
        ):
            print('Invalid media type "{:s}"'.format(m))
            usage()
            return create_dataframe_from_records_meta(records_meta)

    media_types = set(media_types)

    records_available = (
        urllib.request.urlopen(BASE_URL + "&files=records.txt")
        .read()
        .decode("utf-8")
        .splitlines()
    )

    records = list()
    ids = set()
    for r in records_available:
        experiment_type, id, trajectory, date, time = parse_record(r)
        records.append((experiment_type, id, trajectory, date, time))
        ids.add(id)

    ids_requested = set()
    if data_ids:
        for id in data_ids:
            if not re.match(r"^[0-9]{2}$", id):
                print('Invalid id "{:s}"'.format(id))
                usage()
                return records_meta
            if not id in ids:
                print('ID "{:s}" not available'.format(id))
                return records_meta
            ids_requested.add(id)
        ids = ids.intersection(ids_requested)

    ids = list(ids)
    ids.sort()

    for r in records:
        if r[0] in experiment_types and r[1] in ids:
            records_meta.append(r)

    return create_dataframe_from_records_meta(records_meta)
