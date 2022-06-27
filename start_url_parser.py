#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
start_url_parser.py
~~~~~~~~~~~~~~~~~~~

Start parsing image url of parquet files from second component
"""
import os

from url_parser.vk_url_parser import Watcher


def main():
    watch_dir = "data/images/parquet/posts"
    watch = Watcher(watch_directory=watch_dir)
    mode = "listfile"
    # write_mode = "hdfs"
    write_mode = os.getenv("MODE_FS", "local")
    watch.run(fs_mode=mode, write_mode=write_mode)

    
if __name__ == "__main__":
    main()