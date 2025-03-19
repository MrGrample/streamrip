# %%
import os
import json
import requests
import logging
import sys
import shutil


import s3fs
import yandex_music
import yandex_music.utils.request
from yandex_music.exceptions import UnauthorizedError, NotFoundError
import yaml
import pandas as pd
import numpy as np

from tqdm.notebook import tqdm
from pydub import utils as pydub_utls
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import asyncio
import binascii
import hashlib
from Cryptodome.Cipher import AES

from ..config import Config
from ..exceptions import (
    AuthenticationError,
    MissingCredentialsError,
    NonStreamableError,
)

from .client import Client
from .downloadable import YandexDownloadable

logger = logging.getLogger("streamrip")
logging.captureWarnings(True)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"
)

class YandexClient(Client):

    def __init__(self, config: Config):
        self.source = 'yandex'
        self.global_config = config
        self.logged_in = False
        self.config = config.session.yandex
        request = yandex_music.utils.request.Request(headers={"User-Agent": DEFAULT_USER_AGENT}, proxy_url=config.session.downloads.proxy)
        self.client = yandex_music.Client(token=self.config.access_token, request=request, language='en')
        self.proxy = config.session.downloads.proxy

        print('logging')

    async def login(self):
        self.session = await self.get_session()

        try:
            self.client.init()
            self.logged_in = True
            logger.info("Клиент Yandex Music успешно инициализирован.")
        except Exception as e:
            raise ConnectionError(f"Не удалось инициализировать клиент Yandex Music: {e}")

        except Exception as e:
            raise ConnectionError(f"Ошибка при создании клиента Yandex Music: {e}")

    async def get_metadata(self, item_id: str, media_type: str) -> dict:
        if media_type == "track":
            try:
                return await self.get_track(item_id)
            except:
                raise Exception(f"Media type {media_type} not available on yandex")
        elif media_type == "album":
            return await self.get_album(item_id)
        elif media_type == "playlist":
            return await self.get_playlist(item_id)
        elif media_type == "artist":
            return await self.get_artist(item_id)

    async def get_yandex_tracks_lyrics(self, track_id, lyrics_format, attempt=0):
        try:
            lyrics = self.client.tracks_lyrics(track_id, format=lyrics_format)
        except UnauthorizedError:
            return None
        except NotFoundError:
            return None
        except Exception as e:
            if attempt == 10:
                logger.error(f"Ошибка в get_yandex_tracks_lyrics, трек {track_id}, ошибка {e}")
                raise e
            return self.get_yandex_tracks_lyrics(track_id, lyrics_format, attempt + 1)
        print(lyrics)
        return lyrics

    async def get_track_lyrics(self, track_id: str, track: yandex_music.Track) -> tuple:
        try:
            lyrics_format = None
            if track.lyrics_info.has_available_sync_lyrics:
                lyrics_format = 'LRC'
            elif track.lyrics_info.has_available_text_lyrics:
                lyrics_format = 'TEXT'

            if not lyrics_format:
                return None, None

            lyrics = await self.get_yandex_tracks_lyrics(track_id, lyrics_format)
            if lyrics:
                lyrics_metadata = lyrics.to_dict()
                lyrics_metadata['lyric_format'] = lyrics_format
                lyrics_text = await self.yandex_fetch_lyrics(track_id, lyrics)
                lyrics_metadata.pop('download_url')
                logger.info(f'Лирика для трека {track_id} успешно загружена в формате {lyrics_format}')
                return lyrics_metadata, lyrics_text
            else:
                return None, None

        except Exception as e:
            logger.error(f'Ошибка при получении лирики для трека {track_id}: {e}')
            return None, None

    async def yandex_fetch_lyrics(self, track_id, lyrics, attempt=0):
        try:
            lyrics.client = self.client
            lyrics_text = lyrics.fetchLyrics()
        except UnauthorizedError:
            return None
        except NotFoundError:
            return None
        except Exception as e:
            if attempt == 10:
                logger.error(f"Ошибка в yandex_fetch_lyrics, трек {track_id}, ошибка {e}")
                raise e
            return self.yandex_fetch_lyrics(track_id, lyrics, attempt + 1)

        return lyrics_text

    async def get_artist(self, item_id: str) -> dict:
        try:
            artists = await asyncio.to_thread(self.client.artists, artist_ids=item_id)
            albums = await asyncio.to_thread(self.client.artistsDirectAlbums, artist_id=item_id)
            artist = artists[0]
            print(artist)
            metadata = {}
            metadata = albums.to_dict()
            metadata['name'] = artist['name']
            metadata['id'] = artist['id']

            logger.info(f'Метаданные для артиста {item_id} успешно загружены.')
            return metadata
        except Exception as e:
            raise NonStreamableError(e)

    async def get_playlist(self, item_id: str) -> dict:
        #try:
            playlists = await asyncio.to_thread(self.client.users_playlists, user_id = '940441070', kind= item_id)
            print(str(self.client.me['account']['uid']) + ':' + item_id)
            playlist = playlists
            print(playlist)
            metadata = playlist.to_dict()

            logger.info(f'Метаданные для плейлиста {item_id} успешно загружены.')
            return metadata
        #except Exception as e:
            #raise NonStreamableError(e)


    async def get_track(self, item_id: str) -> dict:
        try:
            tracks = await asyncio.to_thread(self.client.tracks, item_id)
            track = tracks[0]

            metadata: dict = {}
            metadata['yandex.track_id'] = item_id
            metadata['yandex.track_metadata'] = track.to_dict()
            metadata['yandex.lyrics_metadata'], metadata['yandex.lyrics_text'] = \
                await self.get_track_lyrics(item_id, track)

            #if not metadata.get('yandex.lyrics_metadata') or not metadata.get('yandex.lyrics_text'):
            #    metadata['yandex.lyrics_metadata'], metadata['yandex.lyrics_text'] = await self.get_track_lyrics(item_id, track)

            logger.info(f'Метаданные для трека {item_id} успешно загружены.')
            return metadata
        except Exception as e:
            raise NonStreamableError(e)

    async def get_album(self, item_id: str) -> dict:
        try:
            albums = await asyncio.to_thread(self.client.albums_with_tracks, item_id)
            print(albums)
            metadata = albums.to_dict()

            logger.info(f'Метаданные для альбома {item_id} успешно загружены.')
            return metadata
        except Exception as e:
            raise NonStreamableError(e)

    async def get_downloadable(self, item_id: str, quality: int = 2, is_retry: bool = False) -> YandexDownloadable:
        if item_id is None:
            raise NonStreamableError(
                "No item id provided. This can happen when searching for fallback songs.",
            )
        # TODO: optimize such that all of the ids are requested at once
        dl_info: dict = {"quality": quality, "id": item_id}

        tracks = await asyncio.to_thread(self.client.tracks, item_id)
        track = tracks[0]

        logger.debug("dz track info: %s", track)

        dl_info["track"] = track
        dl_info["proxy"] = self.proxy
        return YandexDownloadable(self.session, dl_info)

    async def search(self, media_type: str, query: str, limit: int = 200) -> list[dict]:
        # TODO: use limit parameter
        return []







