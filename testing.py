from streamrip.client import DeezerClient
from streamrip.client import YandexClient
from streamrip.config import Config, YandexConfig
import asyncio
from streamrip.media import Album, PendingAlbum, PendingTrack, PendingSingle, PendingPlaylist, PendingArtist
from streamrip.db import Dummy, Database
from streamrip.filepath_utils import clean_filename
import os
import json
import io
import time
import requests
import importlib
import inspect
import random

from concurrent.futures import ThreadPoolExecutor, as_completed
from proxy_provider import ProxyProvider
from proxy_providers import *
from tqdm import tqdm
import csv


SPEEDTEST_URL = "http://212.183.159.230/5MB.zip"

def is_valid_proxy(proxy):
    """Check if the proxy is valid."""
    return proxy.get("host") is not None and proxy.get("country") != "Russia" and proxy.get("country") != "RU"


def construct_proxy_string(proxy):
    """Construct a proxy string from the proxy dictionary."""
    if proxy.get("username"):
        return (
            f'{proxy["username"]}:{proxy["password"]}@{proxy["host"]}:{proxy["port"]}'
        )
    return f'{proxy["host"]}:{proxy["port"]}'


def test_proxy(proxy):
    """Test the proxy by measuring the download time."""
    proxy_str = construct_proxy_string(proxy)
    start_time = time.perf_counter()
    try:
        response = requests.get(
            SPEEDTEST_URL,
            stream=True,
            proxies={"http": f"http://{proxy_str}"},
            timeout=5,
        )
        response.raise_for_status()  # Ensure we raise an error for bad responses

        total_length = response.headers.get("content-length")
        if total_length is None or int(total_length) != 5242880:
            return None

        with io.BytesIO() as f:
            download_time, _ = download_with_progress(
                response, f, total_length, start_time
            )
            return {"time": download_time, **proxy}  # Include original proxy info
    except requests.RequestException:
        return None

def download_with_progress(response, f, total_length, start_time):
    """Download content from the response with progress tracking."""
    downloaded_bytes = 0
    for chunk in response.iter_content(1024):
        downloaded_bytes += len(chunk)
        f.write(chunk)
        done = int(30 * downloaded_bytes / int(total_length))
        if done == 6:
            break
        if (
            done > 3
            and (downloaded_bytes // (time.perf_counter() - start_time) / 100000) < 1.0
        ):
            return float("inf"), downloaded_bytes
    return round(time.perf_counter() - start_time, 2), downloaded_bytes


def save_proxies_to_file(proxies, filename="proxy.json"):
    """Save the best proxies to a JSON file."""
    with open(os.path.join(os.path.dirname(__file__), filename), "w") as f:
        json.dump(proxies, f, indent=4)


def get_best_proxies(providers):
    """Return the top five proxies based on speed from all providers."""
    all_proxies = []
    proxies = None
    for provider in providers:
        try:
            print(f"Fetching proxies from {provider.__class__.__name__}")
            proxies = provider.fetch_proxies()
            all_proxies.extend([proxy for proxy in proxies if is_valid_proxy(proxy)])
        except Exception as e:
            print(f"Failed to fetch proxies from {provider.__class__.__name__}: {e}")

    best_proxies = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(test_proxy, proxy): proxy for proxy in all_proxies}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Testing proxies", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_noinv_fmt}]", unit=' proxies', unit_scale=True, ncols=80):
            result = future.result()
            if result is not None:
                best_proxies.append(result)
    return sorted(best_proxies, key=lambda x: x["time"])[:5]


def update_proxies():
    """Update the proxies list and save the best ones."""
    providers = []
    for filename in os.listdir(os.path.join(os.path.dirname(__file__), "proxy_providers")):
        # Check if the file is a Python module
        if filename.endswith(".py") and filename != "__init__.py":
            module_name = filename[:-3]  # Remove the '.py' suffix
            module_path = f'{"proxy_providers"}.{module_name}'
            module = importlib.import_module(module_path)
            classes = inspect.getmembers(module, inspect.isclass)
            providers.append(
                [classs[-1]() for classs in classes if classs[0] != "ProxyProvider"][0]
            )
    best_proxies = get_best_proxies(providers)
    save_proxies_to_file(best_proxies)
    print("All done.")

#config.session.deezer.arl = '24394a87c336eb060813cd352b1bda714983b79a45f81fbf7a2c89801e0fc857edac902ffe3b2dc7a8de653895973322c33a3315ef968b21e154ee0d53127f6db4852b229da2b27ebe5808dd0f624f3e052d35c673e7cf5a61668fb04605b33d'



db = Database(downloads=Dummy(), failed=Dummy())

async def download_track(track_id, semaphore):
    async with semaphore:
        try:

            config = Config.defaults()
            config.session.yandex.access_token = 'y0__wgBEJvG9uEHGN74BiCH_4GJEiKZCAx-OEFX3SkhW9DNUWMPB36K'

            try:
                with open("proxy.json", "r") as f:
                    proxy = random.choice(json.load(f))
                    proxy_str = construct_proxy_string(proxy)
                    proxy_str = 'http://' + proxy_str
                    print(f"Using proxy from {proxy['city']}, {proxy['country']}")
                    print(proxy_str)

                    config.session.downloads.proxy = proxy_str

                    "set http [proxy]"

                    print("Got 'Sign in to confirm' error. Trying again with another proxy...")
            except FileNotFoundError as e:
                print("'proxy.json' not found. Starting proxy list update...")
                update_proxies()

            # c = DeezerClient(config)

            c = YandexClient(config)

            await c.login()
            print(c.logged_in)

            p = PendingSingle(track_id, client=c, config=config, db=db)
            resolved_track = await p.resolve()
            await resolved_track.rip()
            print(f"Трек {track_id} скачан.")

            await c.session.close()
        except Exception as e:
            print(f"Ошибка при скачивании {track_id}: {str(e)}")


async def main():
    start_time = time.time()

    update_proxies()

    track_ids = []
    with open('lang_zh.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            file_id = row[1].split('/')[-1].split('.')[0]
            track_ids.append(file_id)  # Индекс нужного столбца

    del track_ids[0]

    print(track_ids)

    semaphore = asyncio.Semaphore(100)

    tasks = [download_track(track_id, semaphore) for track_id in track_ids]

    await asyncio.gather(*tasks)


    #my_album = await c.get_metadata("35476145", "track")
    #download = await c.get_downloadable('69480073',2)

    #await download.download(config.session.downloads.folder, None)
    #await download.download(download_path, None)
    #print(my_album)  # should print a giant dictionary with metadata

    #json_object = json.dumps(my_album, indent=4)

   # with open("sample.json", "w") as outfile:
    #    outfile.write(json_object)
    #print(resolved_track.meta)  # print metadata

    end_time = time.time()

    print(end_time - start_time)




  #  my_album = await c.get_metadata("123456", "album")
   # print(my_album)  # should print a giant dictionary with metadata



if __name__ == '__main__':
    print('hi')
    asyncio.run(main())