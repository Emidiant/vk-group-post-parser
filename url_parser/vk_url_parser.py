# -*- coding: utf-8 -*-
"""
vk_url_parser.py
~~~~~~~~~~~~~~~~~

Parsing image url from VK
"""
import io
import os
import time
import filelock

import requests
import pandas as pd
from hdfs import InsecureClient
from vk_common.common_python import get_logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class Watcher:

    def __init__(self, watch_directory: str):
        self.__log = get_logger(self.__class__.__name__)
        self.observer = Observer()
        self.watchDirectory = watch_directory

    def run(self, fs_mode: str, write_mode: str):
        """

        :param fs_mode:      listfile | watchdog
        :param write_mode:   hdfs | local
        """
        self.__log.info(f"Starting url parser in mode {fs_mode} for a directory {self.watchDirectory}")
        event_handler = VkUrlHandler(self.watchDirectory, write_mode=write_mode)
        if fs_mode == "watchdog":
            self.observer.schedule(event_handler, self.watchDirectory, recursive=False)
            self.observer.start()
            try:
                while True:
                    time.sleep(5)
            except:
                self.observer.stop()
                self.__log.info("Observer Stopped")

            self.observer.join()
        elif fs_mode == "listfile":
            try:
                while True:
                    list_files = os.listdir(self.watchDirectory)
                    list_files.sort(key=lambda f: os.path.getmtime(os.path.join(self.watchDirectory, f)))
                    for file_name in list_files:
                        if file_name.startswith("part") and file_name.split(".")[-1] == "parquet":
                            path_to_file = os.path.join(self.watchDirectory, file_name)
                            self.__log.debug(file_name)
                            try:
                                event_handler.parse_parquet(path_to_file)
                            except filelock._error.Timeout:
                                self.__log.debug(f"Skip file: {file_name}, file processing")
                            except FileNotFoundError:
                                self.__log.debug(f"Skip file: {file_name}, file read")
                            except Exception as e:
                                self.__log.warn(f"Error while parsing parquet: {e}")
                    self.__log.debug("Waiting new parquet file")
                    time.sleep(15)
            except Exception as e:
                self.__log.error(e)
            except KeyboardInterrupt:
                self.__log.info("Reading list files stopped")
                return 0


class VkUrlHandler(FileSystemEventHandler):
    """
    URL image parser from VK
    """
    def __init__(self, watch_directory: str, write_mode: str = "local"):
        self.__log = get_logger(self.__class__.__name__)
        self.watchDirectory = watch_directory
        self.write_mode = write_mode
        hdfs_host = os.getenv("HDFS_HOST", "10.32.7.103")
        hdfs_port = os.getenv("HDFS_PORT", "31179")
        self.hdfs_client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user='jusergeeva-242388')
        self.hdfs_dir = os.getenv("HDFS_DIR", "/tmp/jusergeeva-242388/project")

    def on_any_event(self, event):
        """
        Watchdog feature

        :param event:   create | move | modify | delete file in system
        """
        if event.is_directory:
            return None
        elif event.event_type == 'moved' or event.event_type == 'created':
            path_to_file = event.src_path
            file_name = path_to_file.split(os.sep)[-1]
            if file_name.startswith("part") and file_name.split(".")[-1] == "parquet":
                path_to_file = os.path.join(self.watchDirectory, file_name)
                self.__log.debug(f"Watchdog received created event - {path_to_file}")
                self.parse_parquet(path_to_file)

    def parse_parquet(self, path: str) -> None:
        """
        Parse parquet file with links to images

        :param path:    path to parquet file
        """
        file_name = path.split(os.sep)[-1]
        lockfile_name =  "." + file_name.split(".")[0] + ".lock"
        lockfile =os.path.join(self.watchDirectory, lockfile_name)
        with filelock.FileLock(lock_file=lockfile, timeout=10):
            df = pd.read_parquet(path, engine="pyarrow")
            for item in df.to_dict('records'):
                self.image_download(url=item["link"], target=item["target"], id=item["id"],
                                    id_increment=item["id_increment"])
            self.__log.debug(df.head())
            os.remove(path)
        os.remove(lockfile)
        self.__log.debug(f"End process file: {path.split(os.sep)[-1]}")

    def image_download(self, url: str, target: str, id: int, id_increment: int) -> None:
        """
        Download image and save locally | to hdfs

        :param url:             image url
        :param target:          image group type (photo|painting)
        :param id:              image number from source
        :param id_increment:    id post from db
        """
        self.__log.debug(f"Start parse: id_increment={id_increment}, image_id={id}")
        image_path = f"data/images/{target}_2/img_{id}.jpg"
        if self.write_mode == "local":
            os.makedirs(os.path.dirname(image_path), exist_ok=True)
            time.sleep(0.2)
            try:
                img_data = requests.get(url).content
                with open(image_path, 'wb') as img_handler:
                    img_handler.write(img_data)
            except requests.exceptions.ConnectionError as e:
                self.__log.error(e)
                return None
        elif self.write_mode == "hdfs":
            time.sleep(0.2)
            image_path = os.path.join(self.hdfs_dir, f"{target}_2", f"img_{id}.jpg")
            try:
                img_data = requests.get(url).content
                # buf = io.BytesIO()
                # buf.write(img_data)
                # buf.seek(0)
                with self.hdfs_client.write(image_path, overwrite=True) as img_handler:
                    # img_handler.write(buf.getvalue())
                    img_handler.write(img_data)
                # buf.close()
            except requests.exceptions.ConnectionError as e:
                self.__log.error(e)
                return None
