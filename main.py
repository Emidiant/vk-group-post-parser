from parser.vk_groups_parser import VkGroupsParser
from parser.db_handler import DataBaseHandler

def vk_parse():
    db_handler = DataBaseHandler()
    vk_groups_parser = VkGroupsParser()
    domains = db_handler.get_groups_domains()
    for domain, offset in domains:
        resp, new_offset = vk_groups_parser.get_posts(domain, offset=offset, count=2)
        # todo обработка исключений - когда vk начинает блокировать
        df_posts = vk_groups_parser.get_dataframe_from_posts(resp, domain)
        db_handler.upload_posts_dataframe(df_posts, domain)
        db_handler.update_offset(domain, new_offset)

    return 0

if __name__ == "__main__":
    vk_parse()
