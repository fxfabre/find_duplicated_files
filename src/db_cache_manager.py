#!/usr/bin/env python3

import os
from time import time
import pandas as pd
import traceback
from sqlalchemy import create_engine
from typing import Iterable, Dict

from src import io_wrappers
from src.hash_functions import get_hashs_for_file, HASH_FUNCTIONS


class DbCacheManager:

    def __init__(self, data_folder: str):
        super(DbCacheManager, self).__init__()
        self.data_folder = data_folder

    def create_db_cache(self):
        print('Updating cache from folder', self.data_folder)

        df_files_info = pd.DataFrame(self.generate_file_records())
        df_files_info["timestamp"] = time()

        self.save_to_db(df_files_info, 'files_info')
        return df_files_info

    def find_duplicated_files(self):
        df_files_info = self.create_db_cache()

        keys = ['size'] + list(HASH_FUNCTIONS.keys())
        df_duplicated = df_files_info[df_files_info.duplicated(subset=keys)]
        self.save_to_db(df_duplicated, 'duplicated_files')

        return keys, df_duplicated

    def generate_file_records(self) -> Iterable[Dict]:
        for root, file_name in io_wrappers.iter_on_files(self.data_folder):
            try:
                yield self.create_record(root, file_name)
            except FileNotFoundError as e:
                print(e)
                print(root, file_name)

    def create_record(self, folder: str, file_name: str) -> Dict:
        file_path = os.path.join(folder, file_name)
        hash_info = get_hashs_for_file(file_path)
        return dict({
            'name': file_name,
            'folder': os.path.relpath(folder, self.data_folder),
        }, **hash_info)

    def save_to_db(self, df: pd.DataFrame, table_name: str):
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            # TODO : add index ?
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            print(e)
            df.to_csv(table_name + '.csv', index=False)

    @property
    def engine(self):
        if self._engine is None:
            db_user = os.getenv("POSTGRES_USER", "postgres")
            db_pwd = os.getenv("POSTGRES_PASSWORD")
            db_host = os.getenv("POSTGRES_HOST", "localhost")
            self._engine = create_engine(f"postgresql://{db_user}:{db_pwd}@{db_host}:5432/postgres")
        return self._engine
