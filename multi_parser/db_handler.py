from sqlalchemy import inspect, engine, MetaData

from .common_python import get_logger
import credential
import pandas as pd

class DataBaseHandler:

    def __init__(self):
        self.engine = engine.create_engine(f"postgresql://{credential.user}:{credential.password}@{credential.host}:{credential.port}/{credential.database}")
        inspector = inspect(self.engine)
        self.meta_data = MetaData(bind=self.engine.connect())
        MetaData.reflect(self.meta_data)
        self.__log = get_logger(self.__class__.__name__)
        self.__log.debug(f"Tables: {inspector.get_table_names(schema='public')}")

    def get_groups_domains(self) -> list:
        df_groups = pd.read_sql(sql="SELECT * FROM public.groups;", con=self.engine)
        self.__log.debug(df_groups)
        domains = df_groups[["domain", "offset", "type", "allow"]].values.tolist()
        self.__log.debug(f"Domains: {domains}")
        return domains

    def update_offset(self, domain, offset):
        self.__log.debug(f"Update domain: {domain}, offset: {offset}")
        conn = self.engine.connect()
        groups = self.meta_data.tables['groups']
        stmt = groups.update(). \
            values(offset=offset). \
            where(groups.c.domain == domain)
        conn.execute(stmt)

    def upload_posts_dataframe(self, df_posts, domain):
        self.__log.info(f"Start uploading posts from {domain}")
        self.__log.debug(f"\n{df_posts}")
        if df_posts.shape[0] == 0:
            self.__log.warn(f"You have parsed all posts from the group {domain}")
        df_posts.to_sql(name="posts", con=self.engine, if_exists="append", index=False)
