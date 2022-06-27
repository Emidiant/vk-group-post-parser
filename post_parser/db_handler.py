# -*- coding: utf-8 -*-
"""
db_handler.py
~~~~~~~~~~~~~

Operation processing to database
"""
from sqlalchemy import inspect, engine, MetaData, text

from vk_common.common_python import get_logger
import credential
import pandas as pd

class DataBaseHandler:

    def __init__(self, host, port):
        """
        Initial connection to database
        """
        self.__log = get_logger(self.__class__.__name__)
        self.__log.info(f"Start initial DB, host: {host}, port: {port}")
        __list_var_credentials = [item for item in dir(credential) if not item.startswith("__")]
        assert 'user' in __list_var_credentials
        assert 'password' in __list_var_credentials

        self.engine = engine.create_engine(f"postgresql://{credential.user}:{credential.password}@{host}:{port}/{credential.database}")
        inspector = inspect(self.engine)
        self.meta_data = MetaData(bind=self.engine.connect())
        MetaData.reflect(self.meta_data)
        tables = inspector.get_table_names(schema='public')
        self.__log.debug(f"Tables: {tables}")
        if "groups" not in tables:
            self.__log.info("Create table groups")
            with open("sql/group_table_create.sql") as file:
                escaped_sql = text(file.read())
                self.engine.execute(escaped_sql)
        if "posts" not in tables:
            self.__log.info("Create table posts")
            with open("sql/posts_table_create.sql") as file:
                escaped_sql = text(file.read())
                self.engine.execute(escaped_sql)

    def get_groups_domains(self) -> list:
        """
        Get all records from groups table

        :return:       list of tuple ("domain", "offset", "type", "allow", "last_post_timestamp")
        """
        df_groups = pd.read_sql(sql="SELECT * FROM public.groups;", con=self.engine)
        domains = df_groups[["domain", "offset", "type", "allow", "last_post_timestamp"]].values.tolist()
        return domains

    def update_offset(self, domain: str, offset: int) -> None:
        """
        Set new offset to table groups in database

        :param domain:      group string domain
        :param offset:      new int offset
        """
        self.__log.debug(f"Update domain: {domain}, offset: {offset}")
        conn = self.engine.connect()
        groups = self.meta_data.tables['groups']
        stmt = groups.update(). \
            values(offset=offset). \
            where(groups.c.domain == domain)
        conn.execute(stmt)

    def update_timestamp(self, domain: str, timestamp: int) -> None:
        """
        Set new offset to table groups in database

        :param domain:      group string domain
        :param timestamp:   new int timestamp
        """
        self.__log.debug(f"Update domain: {domain}, timestamp: {timestamp}")
        conn = self.engine.connect()
        groups = self.meta_data.tables['groups']
        stmt = groups.update(). \
            values(last_post_timestamp=timestamp). \
            where(groups.c.domain == domain)
        conn.execute(stmt)

    def upload_posts_dataframe(self, df_posts: pd.DataFrame, domain: str, new_offset: int) -> None:
        """
        Upload new data from groups to 'posts' table

        :param df_posts:        dataframe with new data
        :param domain:          group domain
        :param new_offset:      new offset for logging
        """
        if df_posts.shape[0] == 0:
            self.__log.warn(f"You have parsed all posts from the group {domain}")
        else:
            df_posts.to_sql(name="posts", con=self.engine, if_exists="append", index=False)
            self.__log.info(f"Finish uploaded {df_posts.shape[0]} posts from {domain}, New Offset: {new_offset}")
