import datetime
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

    def get_dataframe_from_posts(self, items: List[dict], domain: str, compare_date: int = None, image_parse: bool = False,
                                 target: str ="painting") -> (Union[pd.DataFrame, None], bool):
        """
        Processing VK post

        :param compare_date:    last timestamp from db
        :param items:           list of post in dict format
        :param domain:          group string domain from groups table
        :param image_parse:     flag which activate single mode parsing
        :param target:          type of group: painting or photo
        :return:                processed post in dict format or None if error detected
        """
        self.__log.debug(f"Start processing {len(items)} posts from {domain}")

        processed_items = []
        for post in items:
            if compare_date:
                post_dt = datetime.datetime.fromtimestamp(post["date"])
                if post_dt <= datetime.datetime.fromtimestamp(compare_date):
                    return pd.DataFrame(processed_items), True
            # time.sleep(0.2)
            if "copy_history" in post.keys():
                post_new = post['copy_history'][0]
                post["repost_post_id_from"] = str(post_new["id"]) + str(post_new["from_id"])
                formatted_dict_repost = format_dict(post_new, image_parse=image_parse, target=target)
                formatted_dict_repost["domain"] = domain
                if formatted_dict_repost is None:
                    self.__log.error(f"Stop parsing, problem with connect")
                    return None, False
                processed_items.append(formatted_dict_repost)
            formatted_dict_orig = format_dict(post, image_parse=image_parse, target=target)
            formatted_dict_orig["domain"] = domain
            if formatted_dict_orig is None:
                self.__log.error(f"Stop parsing, problem with connect")
                return None, False
            processed_items.append(formatted_dict_orig)
        return pd.DataFrame(processed_items), False

    def check_new_post(self, resp_two_first_posts: list, last_post_timestamp: int, domain: str) -> (int, int):
        """
        Method for detecting new posts to avoid collisions

        :param resp_two_first_posts:        info about two first posts
        :param last_post_timestamp:         last timestamp from db
        :param domain:                      group domain
        :return:                            start offset (0 - from first post, 1 - from second, -1 - no new post), actual timestamp
        """
        last_datetime = datetime.datetime.fromtimestamp(last_post_timestamp)
        first_post_datetime = datetime.datetime.fromtimestamp(resp_two_first_posts[0]["date"])
        second_post_datetime = datetime.datetime.fromtimestamp(resp_two_first_posts[1]["date"])

        if second_post_datetime > first_post_datetime:
            # значит первый пост закреплённый
            actual_post_dt = second_post_datetime
            actual_timestamp = resp_two_first_posts[1]["date"]
            start_offset = 1
        else:
            # закреплённого поста нет
            actual_post_dt = first_post_datetime
            start_offset = 0
            actual_timestamp = resp_two_first_posts[0]["date"]
        if last_datetime < actual_post_dt:
            self.__log.info(f"Start parsing new posts in group: {domain}. Saved timestamp: {last_datetime}, New timestamp: {actual_post_dt}")
            # необходимо парсить новые данные
            return start_offset, actual_timestamp
        return -1, -1

    def parse_new_post(self, offset: int, domain: str, last_post_timestamp: int, post_batch: int, single_mode: bool, target: str) \
            -> (bool, pd.DataFrame, int, int):
        """
        Первый offset, который приходит, это закреплённая запись, если она есть. Сравниваем datetime первого поста и второго.
        Если второй больше первого, то первый пост закреплённый, проверяем есть ли закреплённый пост в базе по id и домену.
        Сверяем timestamp первого поста в ленте и тот, который сохранён в базе. Если они различаются, то сначала парсим новые посты,
        пока не дойдем до имеющегося. Считаем кол-во новых постов и смещаем offset на это кол-во.

        :param offset:                  saved offset
        :param domain:                  group domain
        :param last_post_timestamp:     int timestamp from db
        :param post_batch:              post amount for api request (max 100)
        :param single_mode:             single mode with image parsing
        :param target:                  type of group (painting or photo)
        :return:
        """
        need_upload_timestamp = False
        df_new_post = pd.DataFrame()
        actual_timestamp = -1

        if not pd.isna(last_post_timestamp):
            self.__log.info(f"Start processing group: {domain}, Timestamp: {datetime.datetime.fromtimestamp(last_post_timestamp)}, Offset: {offset}")
        else:
            self.__log.info(f"Start processing group: {domain}, Offset: {offset}")

        if offset > 0 and not pd.isna(last_post_timestamp):

            resp_two_first_posts, _ = self.get_posts(domain, offset=0, count=2)
            time.sleep(2)
            if len(resp_two_first_posts) == 2:
                offset_check, actual_timestamp = self.check_new_post(resp_two_first_posts, last_post_timestamp, domain)
                if offset_check != -1:
                    num_new_post = 0
                    while True:
                        resp, offset_new = self.get_posts(domain, offset=offset_check, count=post_batch)
                        time.sleep(2)
                        df_posts, find_exist_post = self.get_dataframe_from_posts(resp, domain, compare_date=last_post_timestamp,
                                                                                                image_parse=single_mode,
                                                                                                target=target)
                        if df_posts is None:
                            self.__log.warn("df is None")
                            return -1

                        num_new_post += df_posts.shape[0]
                        if df_posts.shape[0] > 0:
                            if df_new_post.shape[0] == 0:
                                df_new_post = df_posts
                            else:
                                df_new_post = pd.concat([df_new_post, df_posts])
                            # db_handler.upload_posts_dataframe(df_posts, domain)
                        if find_exist_post:
                            # найден пост, совпадающий с последним в базе
                            offset += num_new_post
                            # db_handler.update_offset(domain, offset)
                            # db_handler.update_timestamp(domain, actual_timestamp)
                            self.__log.info(f"Successful find last post. New timestamp: {actual_timestamp}, Num posts: {num_new_post}")
                            break
                        else:
                            offset_check += offset_new
        else:
            need_upload_timestamp = True
        return need_upload_timestamp, df_new_post, offset, actual_timestamp