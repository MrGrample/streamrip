# %%
import os
import json
import requests
import logging
import sys
import shutil


import s3fs
import yandex_music
import yaml
import pandas as pd
import numpy as np


from tqdm.notebook import tqdm
from pydub import utils as pydub_utls
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# %%
# Настройка логгирования
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Пути к файлам логов
error_log_path = os.path.join(log_dir, "errors.log")
info_log_path = os.path.join(log_dir, "info.log")

# Настройка логирования
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Устанавливаем минимальный уровень логирования для логгера

# Обработчик для сообщений ERROR
error_handler = logging.FileHandler(error_log_path)
error_handler.setLevel(logging.ERROR)  # Уровень ERROR для обработчика ошибок
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
error_handler.setFormatter(error_formatter)

# Обработчик для сообщений INFO
info_handler = logging.FileHandler(info_log_path)
info_handler.setLevel(logging.INFO)  # Уровень INFO для обработчика информационных сообщений
info_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
info_handler.setFormatter(info_formatter)

# Добавляем обработчики к логгеру
logger.addHandler(error_handler)
logger.addHandler(info_handler)

# %%
def global_exception_handler(exc_type, exc_value, exc_traceback):
    """Глобальный обработчик ошибок"""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error("Необработанное исключение", exc_info=(exc_type, exc_value, exc_traceback))

# Устанавливаем глобальный обработчик ошибок
sys.excepthook = global_exception_handler


# %%
def init_s3_client(key: str, secret: str, endpoint: str) -> s3fs.S3FileSystem:
    """
    Инициализирует клиент S3.

    Args:
        key (str): Ключ доступа к S3.
        secret (str): Секретный ключ к S3.
        endpoint (str): URL конечной точки S3.

    Returns:
        S3FileSystem: Инициализированный клиент S3.
    """
    try:
        client = s3fs.S3FileSystem(
        anon=False,
        key=key,
        secret=secret,
        endpoint_url=endpoint
    )
        
        # Проверка подключения
        try:
            client.ls('/')
            logger.info("Клиент S3 успешно инициализирован.")
            return client
        except Exception as e:
            raise ConnectionError(f"Не удалось подключиться к S3: \n{e}")
    
    except Exception as e:
        raise ConnectionError(f"Не удалось инициализировать S3 клиент: \n{e}")

# %%
def init_yandex_music_client(access_token: str, headers: Optional[dict] = None,
                             proxy_url: Optional[str] = None) -> yandex_music.Client:
    """
    Инициализирует клиент Yandex Music.
    
    Args:
        access_token (str): Access токен для Yandex Music.
        headers (Optional[dict]): Заголовки запроса. По умолчанию None означает отсутствие заголовков.
        proxy_url (Optional[str]): URL прокси сервера. По умолчанию None означает отсутствие использования прокси.
        
    Returns:
        yandex_music.Client: Инициализированный клиент Yandex Music.
    """
    try:
        if proxy_url:
            request = yandex_music.utils.request.Request(headers=headers, proxy_url=proxy_url)
            client = yandex_music.Client(token=access_token, request=request, language='en')
        else:
            client = yandex_music.Client(token=access_token, language='en')
        
        # Попытка инициализации клиента
        try:
            client.init()
            logger.info("Клиент Yandex Music успешно инициализирован.")
            return client
        except Exception as e:
            raise ConnectionError(f"Не удалось инициализировать клиент Yandex Music: {e}")
            
    except Exception as e:
        raise ConnectionError(f"Ошибка при создании клиента Yandex Music: {e}")

# %%
def load_config(config_path: str) -> dict:
    """
    Загружает конфигурацию из файла.

    Args:
        config_path (str): Путь к файлу конфигурации.

    Returns:
        dict: Конфигурация.

    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def init_config(config_path: str) -> tuple:
    """
    Инициализирует конфигурацию и необходимые клиенты.

    Args:
        config_path (str): Путь к файлу конфигурации.

    Returns:
        tuple: Конфигурация, клиент S3, клиент Yandex Music и DataFrame.

    Raises:
        Exception: Если произошла ошибка при чтении датафрейма.
    """
    config = load_config(config_path)

    s3_client = init_s3_client(key=config['s3']['key'],
                               secret=config['s3']['secret'],
                               endpoint=config['s3']['endpoint'])

    yandex_client = init_yandex_music_client(access_token=config['yandex_music_api']['access_token'],
                                           headers=config['request']['headers'],
                                           proxy_url=config['request']['proxy_url'])

    try:
        id_column = config['source_df']['id_column']
        src_df = pd.read_parquet(path=config['source_df']['path'],
                                columns=[id_column])
        logger.info(f"Датафрейм успешно прочитан. Размер - {len(src_df)} строк")
    except Exception as e:
        raise Exception(f"Ошибка при чтении датафрейма: {e}")

    logger.info('Конфигурационный файл успешно инициализирован.')
    return config, s3_client, yandex_client, src_df

# %%
def get_track_lyrics(yandex_client: yandex_music.Client, track_id: str) -> tuple:
    """
    Получает лирику и метаданные о лирике.
    
    Args:
        yandex_client (Client): Клиент Yandex Music.
        track_id (str): ID трека в Yandex Music.

    Returns:
        tuple: Кортеж из двух элементов:
            - dict: Метаданные о лирике в виде словаря (или пустой словарь, если не удалось получить).
            - str: Текст песни (или пустая строка, если не удалось получить).
    """
    try:
        lyrics_format = ['LRC', 'TEXT']
        lyrics = None

        for format in lyrics_format:
            lyrics = yandex_client.tracks_lyrics(track_id, format=format)

            if lyrics:
                lyrics_metadata = lyrics.to_dict()
                lyrics_metadata['lyric_format'] = format

                lyrics_text = requests.get(lyrics_metadata.pop('download_url')).content.decode()
                logger.info(f'Лирика для трека {track_id} успешно загружена в формате {format}.')
                return lyrics_metadata, lyrics_text

        # Если лирика не найдена ни в одном из форматов, возвращаем пустые значения
        if lyrics is None:
            return None, None

    except Exception as e:
        logger.error(f'Ошибка при получении лирики для трека {track_id}: {e}')
        # Возвращаем пустые значения в случае ошибки
        return None, None

# %%
def get_track_metadata(yandex_client: yandex_music.Client, track: yandex_music.track.track.Track) -> dict:
    """
    Получаем метаданные о треке и лирике, а также синхронизированную лирику
    Args:
        yandex_client (yandex_music.Client): Клиент Yandex Music.
        track (yandex_music.track.track.Track): Объект трека.
        s3_save_dir (str): Путь для сохранения трека на S3.
    
    Returns:
        dict: Словарь с метаданными о треке и лирике.
    """
    try:
        track_id = str(track['id'])

        metadata = {}
        metadata['yandex.track_id'] = track_id
        metadata['yandex.track_metadata'] = track.to_dict()
        metadata['yandex.lyrics_metadata'], metadata['yandex.lyrics_text'] = \
            get_track_lyrics(yandex_client, track_id)
        
        logger.info(f'Метаданные для трека {track_id} успешно загружены.')
        return metadata
    
    except Exception as e:
        logger.error(f"Ошибка при получении метаданных для трека {track_id}: {e}")
        return None

# %%
def download_track(track: yandex_music.track.track.Track, save_path: str) -> bool:
    """
    Скачивает трек с максимально возможным и приемлимым битрейтром.

    Args:
        track (Track): Объект трека из библиотеки yandex_music.
        save_path (str): Путь для сохранения скачанного трека.

    Raises:
        Exception: Если произошла ошибка при загрузке трека.
    """
    try:
        # Получаем информацию о вариантах загрузки трека
        track_id = str(track['id'])
        #track_info = YANDEX_CLIENT.tracks_download_info(track_id)
        #print(track_info)
        
        # Скачиваем лучший битрейт, если 320 недоступен, то качаем 192. Если 192 нет, то вообще не качаем
        bitrates = [320, 192]
        for bitrate in bitrates:
            try:
                track.download(save_path, 'mp3', bitrate)
                logger.info(f"Трек {track_id} скачан с битрейтом {bitrate} kbps как {save_path}")
                return True
            except yandex_music.exceptions.InvalidBitrateError:
                logger.warning(f"Битрейт {bitrate} kbps недоступен для трека {track_id}")
                continue

    except Exception as e:
        logger.error(f"Ошибка при загрузке трека {track_id}: {e}")
        return False
        #raise Exception(f"Ошибка при загрузке трека: {e}")

# %%
def get_audio_info(audio_path: str) -> dict:
    """
    Извлекает метаданные аудио, такие как кодек, частота дискретизации, битрейт, длительность и теги.

    Args:
        audio_path (str): Путь к локальному аудиофайлу.

    Returns:
        dict: Словарь с метаданными аудио или None, если возникли ошибки.
    """
    # if not mime_type.startswith("audio/"):
    #     logging.warning(f"File {os.path.basename(nfs_path)} is not an audio file. Skipping.")
    #     #return {"logs": "Not an audio file"}
    #     return
    
    try:
        # Analyze the audio file using pydub_utils
        audio_info = pydub_utls.mediainfo_json(audio_path)
        return audio_info

    except Exception as e:
        logging.error(f"Ошибка анализа аудио: {os.path.basename(audio_path)} : {e}")
        return None



# %%
def upload_track_to_s3(s3_client: s3fs.S3FileSystem, local_path: str, s3_path: str, overwrite: bool = False):
    """
    Загружает файл на S3.

    Args:
        s3_client (S3FileSystem): Клиент S3.
        local_path (str): Локальный путь к файлу.
        s3_path (str): Путь на S3 для загрузки файла.
        overwrite (bool): Флаг, указывающий, нужно ли перезаписывать существующий файл на S3. По умолчанию False.

    Raises:
        ValueError: Если не указаны local_path или s3_path.
        FileNotFoundError: Если файл не найден по указанному пути local_path.
        Exception: Если произошла ошибка при загрузке файла на S3.
    """
    if not local_path or not s3_path:
        raise ValueError("local_path и s3_path должны быть указаны")

    if not os.path.isfile(local_path):
        logger.error(f"Файл {local_path} не найден")
        raise FileNotFoundError(f"Файл {local_path} не найден")

    # Проверка, существует ли файл на S3 (если не нужно перезаписывать)
    if not overwrite and s3_client.exists(s3_path):
        logger.warning(f"Файл {s3_path} уже существует на S3. Пропускаем загрузку.")
        return s3_path  # Если файл существует, просто возвращаем путь

    try:
        s3_client.upload(local_path, s3_path)
        logger.info(f"Файл {local_path} загружен на S3: {s3_path}")
        return s3_path
    except Exception as e:
        logger.error(f"Ошибка загрузки файла {local_path} в S3: {e}", exc_info=True)
        raise


# %%
def process_track(yandex_client: yandex_music.Client, track_id: str, save_dir: str, s3_client: s3fs.S3FileSystem, s3_save_dir: str):
    """
    Обрабатывает трек: скачивает его, загружает на S3 и удаляет локальную копию.

    Args:
        yandex_client (Client): Клиент Yandex Music.
        track_id (str): Идентификатор трека.
        save_dir (str): Папка для сохранения трека.
        s3_client (S3FileSystem): Клиент S3.
        s3_save_dir (str): Директория на S3 для загрузки.

    Raises:
        Exception: Если произошла ошибка при обработке трека.

    """
    try:
        # Находим трек по ID
        found_track = yandex_client.tracks(track_ids=[track_id])[0]
        track_id_str = str(found_track['id'])
        
        # Собираем метаданные о треке и лирике
        metadata = get_track_metadata(yandex_client, found_track)

        # Загружаем трек на локальный диск
        track_filename = track_id_str + '.mp3'
        save_path = os.path.join(save_dir, track_filename)
        is_downloaded = download_track(found_track, save_path)
        
        if is_downloaded:
            # Анализируем аудио
            audio_info = get_audio_info(save_path)
            metadata['file.media_info'] = audio_info

            # Выгружаем трек на S3
            s3_path = f'{s3_save_dir}{track_id_str}.mp3'
            s3_path_succeeded = upload_track_to_s3(s3_client, save_path, s3_path, overwrite=True)
            metadata['s3_path'] = s3_path_succeeded

            try:
                os.remove(save_path)
            except Exception as e:
                logger.error(f'Ошибка при удалении файла {save_path}: {e}')
        
        logger.info(f'Трек {track_id} полностю обработан.')
        return metadata

    except Exception as e:
        raise Exception(f'Ошибка при обработке трека {track_id}: {e}')


# %%
def convert_dict_columns_to_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Преобразует столбцы со словарями в JSON-строки, чтобы избежать проблем при сохранении в Parquet.

    Args:
        df (pd.DataFrame): Исходный DataFrame.

    Returns:
        pd.DataFrame: DataFrame с преобразованными столбцами.
    """
    df = df.copy()
    for column in df.columns:
        if df[column].apply(lambda x: isinstance(x, dict)).any():
            df[column] = df[column].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x)
    return df

def save_progress(progress_file_path: str, last_index: int, file_counter: int) -> None:
    """
    Функция для надежного сохранения прогресса.

    Args:
        progress_file_path (str): Путь к файлу для сохранения прогресса.
        last_index (int): Последний обработанный индекс.
        file_counter (int): Счетчик файлов.
    """
    progress = {
        'last_processed_index': last_index,
        'file_counter': file_counter
    }
    with open(progress_file_path, "w", encoding='utf-8') as f:
        json.dump(progress, f, indent=4)
    logger.info(f'Прогресс записан: \
                индекс - {last_index}, номер файла - {file_counter}')

# %%
def worker(task_df: pd.DataFrame, id_column: str, chunk_size: int, thread_idx: int,
        yandex_client: yandex_music.Client, save_dir: str,
        s3_client: s3fs.S3FileSystem, s3_save_dir: str) -> None:
    """
    Обрабатывает задачи в отдельном потоке.

    Args:
        task_df (pd.DataFrame): DataFrame с задачами.
        id_column (str): Имя столбца с идентификаторами треков.
        chunk_size (int): Размер чанка для сохранения.
        thread_idx (int): Индекс потока.
        yandex_client (yandex_music.Client): Клиент Yandex Music.
        save_dir (str): Папка для сохранения данных.
        s3_client (s3fs.S3FileSystem): Клиент S3.
        s3_save_dir (str): Директория на S3 для загрузки.

    """
    thread_folder = os.path.join(save_dir, f'thread_{thread_idx}')       # Путь к папке для потока
    progress_file_path = os.path.join(thread_folder, '!progress.json')      # Путь к файлу прогресса

    os.makedirs(thread_folder, exist_ok=True)

    # Читаем прогресс-файл
    progress = {'last_processed_index': 0, 'file_counter': 0}
    if os.path.exists(progress_file_path):
        with open(progress_file_path, 'r', encoding='utf-8') as progress_file:
            progress = json.load(progress_file)

    start_index = progress['last_processed_index']
    file_counter = progress['file_counter']

    buffer = []

    #pbar = progress_bars[thread_idx]  # Берем заранее созданный прогресс-бар
    pbar = tqdm(total=len(task_df), desc=f"Поток {thread_idx}", leave=True, initial = start_index)

    for index, row in task_df.iterrows():
        if index < start_index:
            continue
              
        # Делаем с треком главное :)
        track_id = getattr(row, id_column, None)
        track_data = process_track(yandex_client, track_id, save_dir, s3_client, s3_save_dir)
        
        buffer.append(track_data)

        # Запись по чанкам
        if len(buffer) >= chunk_size:
            thread_df_path = os.path.join(thread_folder, f'chunk_{file_counter}.parquet')
            buffer_df = pd.DataFrame(buffer)
            buffer_df = convert_dict_columns_to_json(buffer_df)
            
            try:
                buffer_df.to_parquet(thread_df_path, index=False)
                file_counter += 1
                #print(f'В save_progress в цикле записываем {index+1}')
                save_progress(progress_file_path, index + 1, file_counter)
                buffer = []
            except Exception as e:
                pbar.close()
                raise Exception(f'Ошибка сохранения {thread_df_path}: {e}')
            

        pbar.update(1)

    # Сохранение остатка буфера
    if buffer:
        thread_df_path = os.path.join(thread_folder, f'chunk_{file_counter}.parquet')
        buffer_df = pd.DataFrame(buffer)
        buffer_df = convert_dict_columns_to_json(buffer_df)

        try:
            buffer_df.to_parquet(thread_df_path, index=False)
            file_counter += 1
        except Exception as e:
            pbar.close()
            print(f'Ошибка сохранения {thread_df_path}: {e}')
    
    # Финальное обновление прогресса
    save_progress(progress_file_path, min(index + 1, len(task_df)), file_counter)
    
    pbar.close()


# %%
def split_src_df_to_tasks(src_df: pd.DataFrame, num_threads: int, tasks_df_dir: str) -> list:
    """
    Разделяет задачу на части, предварительно удаляя старую папку с файлами.

    Args:
        src_df (pd.DataFrame): Исходный DataFrame.
        num_threads (int): Количество потоков.
        save_dir (str): Папка для сохранения данных.

    Returns:
        list: Список DataFrame.
    """
    #tasks_df_path = os.path.join(tasks_df_dir, 'task')
    tasks_df_path = tasks_df_dir
    
    # Полностью удаляем папку с таск_датафреймами, если она существует
    if os.path.exists(tasks_df_path):
        shutil.rmtree(tasks_df_path)

    # Создаём пустую папку заново
    os.makedirs(tasks_df_path, exist_ok=True)
    
    # Разбиваем DataFrame и сохраняем новые части
    tasks_df = np.array_split(src_df, num_threads)
    for index, df_part in enumerate(tasks_df):
        df_part_path = os.path.join(tasks_df_path, f'thread_{index}.parquet')
        df_part.to_parquet(df_part_path)
    
    return tasks_df


def load_tasks_df(tasks_dir: str) -> list:
    """
    Загружает DataFrame с задачами из файлов .parquet.
    Args:
        tasks_dir (str): Папка с файлами .parquet.

    Returns:
        list: Список DataFrame.

    Raises:
        FileNotFoundError: Если не найдены файлы .parquet.
    """
    tasks_df = []
    parquet_files = [
        f for f in os.listdir(tasks_dir) 
        if f.endswith('.parquet') and not f.startswith(('~', '.', 'Thumbs', 'desktop'))
    ]

    if not parquet_files:
        #logger.error(f'Не найдено файлов с расширением .parquet {tasks_dir}')
        raise FileNotFoundError(f'Не найдено файлов с расширением .parquet {tasks_dir}')

    for file in parquet_files:
        df_part_path = os.path.join(tasks_dir, file)
        tasks_df.append(pd.read_parquet(df_part_path))

    return tasks_df

# %%
def main():
    try:
        logger.info('Запуск приложения...')
        
        # Подгружаем конфигурационный файл
        # инициализируем подключение к Yandex и S3
        # Получаем исходный датафрейм для последующей обработки
        config, s3_client, yandex_client, src_df = init_config('cfg/config-default.yaml')

        # Настраиваем переменные из конфигурационного файла
        id_column = config['source_df']['id_column']
        s3_save_dir = config['s3']['save_dir']
        save_dir = config['tasks']['save_dir']
        num_threads = config['tasks']['num_threads']
        chunk_size = config['tasks']['chunk_size']
        tasks_df_dir = config['tasks']['tasks_df_dir']
        separate = config['tasks']['separate']

        if separate is True:
            # Делим исходный датафрейм на части и сохраняем их в папку
            tasks_df = split_src_df_to_tasks(src_df, num_threads, tasks_df_dir)
        else:
            # Подгружаем уже заспличенные датафреймы в работу
            tasks_df = load_tasks_df(tasks_df_dir)

        # Создаём прогресс-бары для потоков заранее, чтобы сохранить их порядок в Jupyter
        #progress_bars = [tqdm(total=len(tasks_df[i]), desc=f"Поток {i}", position=i, leave=True) for i in range(num_threads)]

        # Флаг для контроля выполнения
        running = True

        try:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {
                    executor.submit(worker, tasks_df[thread_idx], id_column, chunk_size, thread_idx,
                                    yandex_client, save_dir,
                                    s3_client, s3_save_dir): tasks_df[thread_idx]
                    for thread_idx in range(num_threads)
                }

                try:
                    for future in as_completed(futures):
                        if not running: # Проверяем флаг запущенности приложения
                            break
                        try:
                            future.result()  # Если worker кидает исключение, оно всплывет здесь
                        except Exception as e:
                            print(f"Ошибка в потоке {future}: {e}")

                except KeyboardInterrupt:
                    running = False  # Меняем флаг
                    logger.info("Прерывание работы...")
                    # Отменяем все незавершенные задачи
                    for future in futures:
                        if not future.done():
                            future.cancel()
                    # Ждем завершения текущих задач
                    executor.shutdown(wait=True)
        
        
        except Exception as e:
            logger.error(f"Ошибка при выполнении задач: {e}", exc_info=True)

        finally:
            logger.info('Процесс завершен')
    
    except Exception as e:
        logger.error(f'Критическая ошибка в main(): {e}', exc_info=True)
        sys.exit(1)



