# -*- coding: utf-8 -*-

"""
common_spark.py
~~~~~~~~~~~~~~~

Useful functions of Python
"""
import json
import time
from typing import Union
import requests
import logging
import colorlog


def get_logger(class_name, log_level=logging.DEBUG):
    logger = logging.Logger(name=class_name, level=log_level)
    stream_handler = colorlog.StreamHandler()
    stream_handler.setFormatter(
        colorlog.ColoredFormatter('%(log_color)s%(asctime)s  %(name)16s  %(levelname)8s: %(message)s'))
    logger.addHandler(stream_handler)
    return logger


def find_optimal_image_size(sizes_link_list: list) -> Union[dict, None]:
    """

    :param sizes_link_list:
    :return:
    """
    height_or_width_optimal = 604
    min_diff, find_item = height_or_width_optimal, []

    for item in sizes_link_list:
        if item["height"] > item["width"]:
            max_size = "height"
        else:
            max_size = "width"
        if abs(int(item[max_size]) - height_or_width_optimal) < min_diff:
            min_diff = int(item[max_size])
            find_item = item
    if isinstance(find_item, dict):
        if "url" in find_item.keys():
            return find_item["url"]
    return None

def format_dict(item: dict, image_parse: bool = False, target: str = "painting") -> Union[dict, None]:
    """
    Post data preparing for insert in database. Single mode with image parsing allowed for old version not parallel model

    :param image_parse:     activated flag for single mode parser
    :param target:          group type, which we're parsing painting or photo
    :param item:            post data in dictionary format
    :return:                transformed dictionary with data from post
    """
    columns = ["id", "from_id", "owner_id", "date", "marked_as_ads", "is_favorite", "post_type", "text", "attachments", "post_source", "repost_post_id_from"]
    new_dict = {col: "" for col in columns}
    for col in columns:
        if col in item.keys():
            if col == "attachments":
                photo_list = []
                photo_counter = 0
                for att in item[col]:
                    if att["type"] == "photo":
                        photo_counter += 1
                        photo_dict = {
                            "id": att["photo"]["id"],
                            "date": att["photo"]["id"],
                            "link": find_optimal_image_size(att["photo"]["sizes"])
                        }
                        if photo_dict["link"]:
                            # start image parsing for single mode
                            if image_parse:
                                image_path = f"data/images/{target}/img_{att['photo']['id']}.jpg"
                                photo_dict["path"] = image_path
                                time.sleep(0.2)
                                try:
                                    img_data = requests.get(photo_dict["link"]).content
                                    with open(image_path, 'wb') as handler:
                                        handler.write(img_data)
                                except requests.exceptions.ConnectionError as e:
                                    return None
                            photo_list.append(photo_dict)

                new_dict[col] = str(photo_list)
            elif isinstance(item[col], dict):
                new_dict[col] = json.dumps(item[col])
            elif isinstance(item[col], list):
                new_dict[col] = str(item[col])
            else:
                new_dict[col] = item[col]
    process_count_col = ["likes", "views", "reposts"]
    for count_col in process_count_col:
        if count_col in item.keys():
            new_dict[f"{count_col}_count"] = item[count_col]["count"]
        else:
            new_dict[f"{count_col}_count"] = 0

    return new_dict
