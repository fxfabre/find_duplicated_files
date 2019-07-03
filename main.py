# coding=utf-8

import os
import hashlib
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import traceback
from sqlalchemy import create_engine

FILES_TO_IGNORE = ['.DS_Store']
SIZE_READ = 1024 * 1024


class DuplicateFilesFinder:

    def __init__(self):
        super(DuplicateFilesFinder, self).__init__()
        load_dotenv(dotenv_path=Path.cwd() / '.env')
        self._engine = None
        self.hash_functions = {'md5': hashlib.md5, 'sha256': hashlib.sha256}

    def main(self):
        files_info = []
        for folder, file_name, hash_info in self.get_hashs():
            try:
                files_info.append(self.create_record(folder, file_name, hash_info))
            except FileNotFoundError:
                pass

        df_files_info = pd.DataFrame(files_info)
        self.save_to_db(df_files_info, 'files_info')

        # find duplicated
        df_duplicated = df_files_info[df_files_info.duplicated(subset=['size'] + list(self.hash_functions.keys()))]
        self.save_to_db(df_duplicated, 'duplicated_files')

        self.drop_duplicated_files(df_files_info)

        print('duplicated :')
        print(df_duplicated)

    def get_hashs(self):
        print('folder', self.folder_documents)
        for root, dirs, files in os.walk(self.folder_documents):
            print(root)
            files = [f for f in files if f not in FILES_TO_IGNORE]
            for file_name in files:
                try:
                    file_path = os.path.join(root, file_name)
                    if self.keep_file(file_path):
                        yield root, file_name, self.get_hashs_for_file(file_path)
                except FileNotFoundError as e:
                    print(e)
                    print(file_path)

    def get_hashs_for_file(self, fname):
        hashs = {func_name: func() for func_name, func in self.hash_functions.items()}
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(SIZE_READ), b""):
                for hash_name in hashs.keys():
                    hashs[hash_name].update(chunk)

        return {hash_name: hash_func.hexdigest() for hash_name, hash_func in hashs.items()}

    def keep_file(self, file_path):
        return os.path.getsize(file_path) > 1e6

    def create_record(self, folder, file_name, hash_info):
        return dict({
            'size': os.path.getsize(os.path.join(folder, file_name)),
            'name': file_name,
            'folder': os.path.relpath(folder, self.folder_documents),
        }, **hash_info)

    def save_to_db(self, df: pd.DataFrame, table_name: str):
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            print(e)
            df.to_csv(table_name + '.csv', index=False)

    def drop_duplicated_files(self, df: pd.DataFrame):
        """
        Drop files with:
        - the shortest folder : longest folder often have useless names like 'to_sort' or 'to_clean'
        - the longest name : keep maximum information
        """
        df['name_len'] = -df['name'].map(len)
        df['folder_len'] = df['folder'].map(len)
        for files_info, sub_df in df.groupby(['size'] + list(self.hash_functions.keys())):
            sub_df = sub_df.sort_values(['folder_len', 'name_len'])
            for idx, row in sub_df.iloc[1:, :].iterrows():
                print('remove', row['folder'], row['name'])

    @property
    def engine(self):
        if self._engine is None:
            pwd = os.getenv('POSTGRES_PASSWORD')
            self._engine = create_engine('postgresql://postgres:{pwd}@localhost:5432/postgres'.format(pwd=pwd))
        return self._engine

    @property
    def folder_documents(self):
        return os.getenv('FOLDER_DOCUMENTS')


if __name__ == '__main__':
    DuplicateFilesFinder().main()
