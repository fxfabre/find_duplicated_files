#!/usr/bin/env python3
import os
import traceback
from ctypes import c_ulong
from time import time
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd
from numpy import base_repr
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils.functions import create_database
from sqlalchemy_utils.functions import database_exists

from . import Base
from . import Folders
from src.io_files.hash_functions import FileHashManager
from src.io_files.hash_functions import HASH_FUNCTIONS


class DbCacheManager:
    def __init__(self, hash_gen: FileHashManager):
        super(DbCacheManager, self).__init__()
        self.hash_gen = hash_gen
        self._session_maker = None
        self._table_name = None
        self._db_cache = None
        self._metadata = None
        self._engine = None
        self._set_up_db()

    def _set_up_db(self):
        self._metadata = Base.metadata
        self.engine.connect().execution_options(autocommit=True)
        if not database_exists(url=self.engine.url):
            create_database(url=self.engine.url, encoding="utf8")
        self._metadata.create_all(self.engine, checkfirst=True)

    def read_or_create_cache(self) -> pd.DataFrame:
        df_table = self._try_read_from_db()
        if df_table is None:
            return self._create_db_cache()
        return df_table

    def find_file(
        self, file_path: str, hash_generator: FileHashManager
    ) -> Tuple[str, str]:
        file_hash_info = hash_generator.get_hashs_for_file(file_path)
        query = " and ".join([f"{k} == {v!r}" for k, v in file_hash_info.items()])
        existing_file = self.db_cache.query(query)

        if existing_file.shape[0] > 0:
            return existing_file["folder"].iloc[0], existing_file["name"].iloc[0]
        else:
            return "", ""

    def find_duplicated_files(self) -> Tuple[List[str], pd.DataFrame]:
        df_files_info = self.read_or_create_cache()

        keys = ["size"] + list(HASH_FUNCTIONS.keys())
        df_duplicated = df_files_info[df_files_info.duplicated(subset=keys)]
        self.save_to_db(df_duplicated, "duplicated_files")

        return keys, df_duplicated

    def save_to_db(self, df: pd.DataFrame, table_name: str) -> None:
        try:
            df.to_sql(table_name, self.engine, if_exists="replace", index=False)
            self._update_db_metadata()
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            print(e)
            df.to_csv(table_name + ".csv", index=False)

    def _try_read_from_db(self) -> Optional[pd.DataFrame]:
        if self.table_name in self.tables:
            return pd.read_sql_table(self.table_name, self.engine)
        return None

    def _create_db_cache(self) -> pd.DataFrame:
        print("Create cache from folder", self.data_folder)

        file_hash_manager = FileHashManager(self.data_folder)
        df_files_info = pd.DataFrame(file_hash_manager.generate_file_records())
        df_files_info["timestamp"] = time()

        self.save_to_db(df_files_info, self.table_name)
        return df_files_info

    def _update_db_metadata(self) -> None:
        if not self._metadata.is_bound():
            self._metadata.bind = self.engine
        self._metadata.reflect()

    @property
    def table_name(self) -> str:
        if self._table_name:
            return self._table_name

        db_folder: Folders = self.session.query(Folders).filter_by(
            folder_name=self.data_folder
        ).first()
        if db_folder is None:
            hash_value = hash(self.data_folder)
            hash_value = hash_value if hash_value >= 0 else c_ulong(hash_value).value
            db_folder = Folders(
                folder_name=self.data_folder, table_name=base_repr(hash_value, 36)
            )

            session = self.session
            session.add(db_folder)
            session.commit()

        self._table_name = db_folder.table_name
        return self._table_name

    @property
    def engine(self):
        if self._engine is None:
            db_user = os.getenv("POSTGRES_USER", "postgres")
            db_pwd = os.getenv("POSTGRES_PASSWORD")
            db_host = os.getenv("POSTGRES_HOST", "localhost")
            self._engine = create_engine(
                f"postgresql://{db_user}:{db_pwd}@{db_host}:5432/postgres"
            )
        return self._engine

    @property
    def tables(self):
        if not self._metadata.is_bound():
            self._update_db_metadata()
        return self._metadata.tables

    @property
    def data_folder(self) -> str:
        return self.hash_gen.data_folder

    @property
    def db_cache(self) -> pd.DataFrame:
        if self._db_cache is None:
            self._db_cache = self.read_or_create_cache()
        return self._db_cache

    @property
    def session(self):
        if self._session_maker is None:
            self._session_maker = sessionmaker(bind=self.engine)
        return self._session_maker()
