import datetime
import time
from time import sleep

from multi_parser.vk_post_parser import VkPostParser
from multi_parser.db_handler import DataBaseHandler


def vk_parse(single_mode: bool = False, post_batch: int = 5):
    db_handler = DataBaseHandler()
    vk_groups_parser = VkPostParser()
    total_loop = 100
    i = total_loop
    try:
        while i > 0:
            print(f"Iteration {total_loop - i + 1}/{total_loop}")
            domains = db_handler.get_groups_domains()
            for domain, offset, target, allow, last_post_timestamp in domains:
                if allow:
                    need_upload_timestamp, df_new_post, offset, actual_timestamp = \
                        vk_groups_parser.parse_new_post(offset, domain, last_post_timestamp, post_batch, single_mode, target)

                    if df_new_post.shape[0] != 0:
                        # найден пост, совпадающий с последним в базе
                        db_handler.upload_posts_dataframe(df_new_post, domain, offset)
                        db_handler.update_offset(domain, offset)
                        db_handler.update_timestamp(domain, actual_timestamp)

                    # parsing old posts
                    resp, new_offset = vk_groups_parser.get_posts(domain, offset=offset, count=post_batch)
                    if new_offset >= 0:
                        df_posts, _ = vk_groups_parser.get_dataframe_from_posts(resp, domain, image_parse=single_mode, target=target)
                        if df_posts is None or df_posts.shape[0] == 0:
                            print("problem")
                            return -1
                        if need_upload_timestamp:
                            db_handler.update_timestamp(domain, max(df_posts["date"]))
                        db_handler.upload_posts_dataframe(df_posts, domain, new_offset)
                        db_handler.update_offset(domain, new_offset)
                        sleep(2)
                    elif new_offset == -1:
                        print("problem")
                        return -1
            i -= 1
    except KeyboardInterrupt:
        print("Parser stopped")
    return 0

if __name__ == "__main__":
    vk_parse(single_mode=False, post_batch=5)
