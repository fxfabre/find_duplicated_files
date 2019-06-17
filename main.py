# coding=utf-8

import os
import hashlib
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine

FILES_TO_IGNORE = ['.DS_Store']
hash_functions = {'md5': hashlib.md5, 'sha256': hashlib.sha256}
SIZE_READ = 1024 * 1024


def get_engine():
    pwd = os.getenv('POSTGRES_PASSWORD')
    return create_engine('postgresql://postgres:{pwd}@localhost:5432/postgres'.format(pwd=pwd))


def get_hashs_for_file(fname):
    hashs = {func_name: func() for func_name, func in hash_functions.items()}
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(SIZE_READ), b""):
            for hash_name in hashs.keys():
                hashs[hash_name].update(chunk)

    return {hash_name: hash_func.hexdigest() for hash_name, hash_func in hashs.items()}


def get_hashs():
    folder_documents = os.getenv('FOLDER_DOCUMENTS')
    print('folder', folder_documents)
    for root, dirs, files in os.walk(folder_documents):
        print(root)
        files = [f for f in files if f not in FILES_TO_IGNORE]
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if os.path.getsize(file_path) < 1e6:
                continue
            yield root, file_name, get_hashs_for_file(file_path)


def create_record(folder, file_name, hash_info):
    return dict({
        'size': os.path.getsize(os.path.join(folder, file_name)),
        'name': file_name,
        'folder': folder,
    }, **hash_info)


def main():
    load_dotenv(dotenv_path=Path.cwd() / '.env')
    engine = get_engine()

    files_info = []
    for folder, file_name, hash_info in get_hashs():
        files_info.append(create_record(folder, file_name, hash_info))

    df_files_info = pd.DataFrame(files_info)
    df_files_info.to_sql('files_info', engine, if_exists='replace', index=False)

    # find duplicated
    df_duplicated = df_files_info[df_files_info.duplicated(subset=['size'] + list(hash_functions.keys()))]
    df_duplicated.to_sql('duplicated_files', engine, if_exists='replace', index=False)

    print('duplicated :')
    print(df_duplicated)


if __name__ == '__main__':
    main()
