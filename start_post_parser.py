#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
start_post_parser.py
~~~~~~~~~~~~~~~~~~~~

Launching the post parser
"""
import os
from time import sleep

import credential
from post_parser.vk_post_parser import VkPostParser
from post_parser.db_handler import DataBaseHandler
from vk_common.common_python import get_logger


def vk_post_parse(single_mode: bool = False):
    __log = get_logger("VkPostParse")
    post_batch = os.getenv("POST_BATCH", 5)
    if isinstance(post_batch, str):
        try:
            post_batch = int(post_batch)
        except Exception as e:
            __log.error(e)
            post_batch = 5
    db_handler = DataBaseHandler(
        host=os.getenv("POSTGRES_HOST", credential.host),
        port=os.getenv("POSTGRES_PORT", credential.port)
    )
    hdfs_dir = os.getenv("HDFS_DIR", "/tmp/jusergeeva-242388/project")
    vk_groups_parser = VkPostParser(hdfs_dir)
    total_loop = 100
    i = total_loop

    try:
        while i > 0:
            __log.info(f"Iteration {total_loop - i + 1}/{total_loop}")
            domains = db_handler.get_groups_domains()
            for domain, offset, target, allow, last_post_timestamp in domains:
                if allow:
                    # check new posts
                    need_upload_timestamp, df_new_post, offset, actual_timestamp = \
                        vk_groups_parser.parse_new_post(offset, domain, last_post_timestamp, post_batch, single_mode, target)

                    if df_new_post.shape[0] != 0:
                        # found a post matching the last one in the database
                        df_new_post["target"] = target
                        db_handler.upload_posts_dataframe(df_new_post, domain, offset)
                        db_handler.update_offset(domain, offset)
                        db_handler.update_timestamp(domain, actual_timestamp)

                    # parsing old posts
                    resp, new_offset = vk_groups_parser.get_posts(domain, offset=offset, count=post_batch)
                    if new_offset >= 0:
                        df_posts, _ = vk_groups_parser.get_dataframe_from_posts(resp, domain, image_parse=single_mode, target=target)
                        if df_posts is None or df_posts.shape[0] == 0:
                            __log.warning("Read all posts from the group or an error has occurred")
                            continue

                        if need_upload_timestamp:
                            db_handler.update_timestamp(domain, max(df_posts["date"]))
                        df_posts["target"] = target
                        db_handler.upload_posts_dataframe(df_posts, domain, new_offset)
                        db_handler.update_offset(domain, new_offset)
                        sleep(2)
                    elif new_offset == -1:
                        __log.error("Problem, new_offset = -1")
                        return -1
            i -= 1
    except KeyboardInterrupt:
        __log.info("Parser stopped")
    return 0

if __name__ == "__main__":
    vk_post_parse(single_mode=False)
