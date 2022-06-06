import json
import time
from typing import List, Union

import requests
import credential
import pandas as pd

from multi_parser.common_python import format_dict, get_logger


class VkPostParser:
    """
    Post parser from group by domain
    """

    def __init__(self):
        self.__log = get_logger(self.__class__.__name__)
        self.__access_token = credential.access_token

    def get_new_access_token(self) -> int:
        """
        Get new token for vk

        :return:    operation code
        """
        redirect_uri = "https://oauth.vk.com/blank.html"
        code_url = f"https://oauth.vk.com/authorize?client_id={credential.app_id}&display=page&redirect_uri={redirect_uri}&scope=groups&response_type=code&v=5.131"
        self.__log.debug(f"Please, follow link and update code: {code_url}")
        # Параметр code может быть использован в течение 1 часа для получения ключа доступа к API access_token с Вашего сервера.
        url_token = f"https://oauth.vk.com/access_token?client_id=8166774&client_secret={credential.client_secret}&redirect_uri={redirect_uri}&code={credential.code}"
        response = json.loads(requests.get(url_token).text)
        self.__log.debug(response)
        if "access_token" in response.keys():
            self.__log.info("Token successfully updated")
            self.__access_token = response["access_token"]
            credential.access_token = response["access_token"]
            return 0
        else:
            self.__log.error(f"Can't update token: {response}")
            return -1

    def get_posts(self, group_domain: str, offset: int = 0, count: int = 10) -> (list, int):
        f"""
        Get post with API vk
        
        :param group_domain:    group string domain vk.com/<group_domain>
        :param offset:          ordinal number of the post from which parsing in the group starts
        :param count:           post amount, which returned from API, max 100
        :return:                list of posts in dictionary format with offset. offset = -1 - error while response, -2 - token expired
        """
        response = requests.get('https://api.vk.com/method/wall.get', params={
                'access_token': self.__access_token,
                'v': 5.131,
                "domain": group_domain,
                'offset': offset,
                # согласно документации api - максимум 100 за раз
                "count": count
            }).json()
        if "response" in response.keys():
            response = response["response"]
        else:
            self.__log.error(response['error'])
            if "access_token has expired" in response['error']["error_msg"]:
                self.__log.warn("You need to update your token!")
                if self.get_new_access_token() == 0:
                    return [], -2
            return [], -1
        self.__log.debug(f"Items amount: {len(response['items'])}")
        return response['items'], offset + len(response['items'])

    def get_dataframe_from_posts(self, items: List[dict], domain: str, image_parse: bool = False, target: str ="painting") -> Union[pd.DataFrame, None]:
        """
        Processing VK post

        :param items:           list of post in dict format
        :param domain:          group string domain from groups table
        :param image_parse:     flag which activate single mode parsing
        :param target:          type of group: painting or photo
        :return:                processed post in dict format or None if error detected
        """
        self.__log.info(f"Start processing {len(items)} posts from {domain}")

        processed_items = []
        for post in items:
            time.sleep(0.2)
            preview_text = post['text'][:80].replace('\n', '\\n')
            if "copy_history" in post.keys():
                preview_text += ", copy_history: " + post['copy_history'][0]['text'][:80].replace('\n', '\\n')
                post_new = post['copy_history'][0]
                post["repost_post_id_from"] = str(post_new["id"]) + str(post_new["from_id"])
                formatted_dict_repost = format_dict(post_new, image_parse=image_parse, target=target)
                formatted_dict_repost["domain"] = domain
                if formatted_dict_repost is None:
                    self.__log.error(f"Stop parsing, problem with connect")
                    return None
                processed_items.append(formatted_dict_repost)
            formatted_dict_orig = format_dict(post, image_parse=image_parse, target=target)
            formatted_dict_orig["domain"] = domain
            if formatted_dict_orig is None:
                self.__log.error(f"Stop parsing, problem with connect")
                return None
            processed_items.append(formatted_dict_orig)
            # todo сохранение последнего прочитанного id поста, чтобы избежать коллизий при появлении новых записей в группе
            # self.__log.debug(f"id: {post['id']}, date: {datetime.datetime.fromtimestamp(int(post['date']))}, text: {preview_text}")
        return pd.DataFrame(processed_items)
