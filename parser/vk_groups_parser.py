import json

import requests
import credential
import pandas as pd
import datetime
from parser.common import get_logger


class VkGroupsParser:

    def __init__(self):
        self.__log = get_logger(self.__class__.__name__)

    def get_posts(self, group_domain: str, offset: int = 0, count: int = 10) -> (list, int):
        response = requests.get('https://api.vk.com/method/wall.get', params={
                'access_token': credential.access_token,
                'v': 5.131,
                "domain": group_domain,
                'offset': offset,
                # согласно документации api - максимум 100 за раз
                "count":count
            }).json()
        if "response" in response.keys():
            response = response["response"]
        else:
            self.__log.error(response['error'])
            return []
        self.__log.debug(f"Items amount: {len(response['items'])}")
        return response['items'], offset + len(response['items'])

    def get_dataframe_from_posts(self, items, domain) -> pd.DataFrame:
        self.__log.info(f"Start processing {len(items)} posts from {domain}")

        def __find_large_size(sizes_link_list: list) -> str:
            max_size, max_item = 0, []
            for item in sizes_link_list:
                if int(item["height"]) * int(item["width"]) > max_size:
                    max_size = int(item["height"]) * int(item["width"])
                    max_item = item
            return max_item["url"]

        def __format_dict(item: dict) -> dict:
            columns = ["id", "from_id", "owner_id", "date", "marked_as_ads", "is_favorite", "post_type", "text", "attachments", "post_source", "repost_post_id_from"]
            new_dict = {col: "" for col in columns}
            for col in columns:
                if col in item.keys():
                    if col == "attachments":
                        photo_list = []
                        for att in item[col]:
                            if att["type"] == "photo":
                                photo_dict = {"id": att["photo"]["id"], "date": att["photo"]["id"], "link": __find_large_size(att["photo"]["sizes"])}
                                self.__log.debug(f"start processing image {att['photo']['id']}")
                                # todo данный блок выносится в отдельный парсер, который заменяет в постах url на пути к изображению, когда оно сохраняется на диске
                                #   делает он это в своём темпе и ссылки получает из очереди
                                img_data = requests.get(photo_dict["link"]).content
                                with open(f"data/images/img_{att['photo']['id']}.jpg", 'wb') as handler:
                                    handler.write(img_data)
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

        processed_items = []
        for post in items:
            preview_text = post['text'][:80].replace('\n', '\\n')
            if "copy_history" in post.keys():
                preview_text += ", copy_history: " + post['copy_history'][0]['text'][:80].replace('\n', '\\n')
                post_new = post['copy_history'][0]
                post["repost_post_id_from"] = str(post_new["id"]) + str(post_new["from_id"])
                formatted_dict_repost = __format_dict(post_new)
                processed_items.append(formatted_dict_repost)
            formatted_dict_orig = __format_dict(post)
            processed_items.append(formatted_dict_orig)
            # todo сохранение последнего прочитанного id поста, чтобы избежать коллизий при появлении новых записей в группе
            # self.__log.debug(f"id: {post['id']}, date: {datetime.datetime.fromtimestamp(int(post['date']))}, text: {preview_text}")
        return pd.DataFrame(processed_items)