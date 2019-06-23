# coding=utf-8

import os
import hashlib
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
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
            files_info.append(self.create_record(folder, file_name, hash_info))

        df_files_info = pd.DataFrame(files_info)
        df_files_info.to_sql('files_info', self.engine, if_exists='replace', index=False)

        # find duplicated
        df_duplicated = df_files_info[df_files_info.duplicated(subset=['size'] + list(self.hash_functions.keys()))]
        df_duplicated.to_sql('duplicated_files', self.engine, if_exists='replace', index=False)

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
