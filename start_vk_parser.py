from time import sleep

from multi_parser.vk_post_parser import VkPostParser
from multi_parser.db_handler import DataBaseHandler


def vk_parse(single_mode: bool = False):
    db_handler = DataBaseHandler()
    vk_groups_parser = VkPostParser()
    total_loop = 100
    i = total_loop
    try:
        while i > 0:
            print(f"Iteration {total_loop - i + 1}/{total_loop}")
            domains = db_handler.get_groups_domains()
            for domain, offset, target, allow in domains:
                if allow:
                    resp, new_offset = vk_groups_parser.get_posts(domain, offset=offset, count=5)
                    if new_offset >= 0:
                        df_posts = vk_groups_parser.get_dataframe_from_posts(resp, domain, image_parse=single_mode, target=target)
                        if df_posts is None:
                            return -1
                        db_handler.upload_posts_dataframe(df_posts, domain)
                        db_handler.update_offset(domain, new_offset)
                        sleep(5)
                    elif new_offset == -1:
                        return -1

            i -= 1
    except KeyboardInterrupt:
        print("Parser stopped")
    return 0

if __name__ == "__main__":
    vk_parse(single_mode=True)
