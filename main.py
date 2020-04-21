#!/usr/bin/env python3

import os
import sys
import shutil
from dotenv import load_dotenv
from pathlib import Path

from src.db_cache_manager import DbCacheManager
from src import io_wrappers
from src import hash_functions


def copy_recursive(source_folder: str, target_folder: str):
    source_folder = os.path.abspath(source_folder)
    target_folder = os.path.abspath(target_folder)

    db_cache_manager = DbCacheManager(target_folder)
    df_files_info = db_cache_manager.create_db_cache()
    file_manager = io_wrappers.FileManager()

    for root, file_name in io_wrappers.iter_on_files(source_folder):
        file_path = os.path.join(root, file_name)
        relative_folder = os.path.relpath(root, source_folder)
        relative_path = os.path.join(relative_folder, file_name)

        hash_info = hash_functions.get_hashs_for_file(file_path)
        query = ' and '.join([
            f"{k} == {v!r}" for k, v in hash_info.items()
        ])
        existing_file = df_files_info.query(query)

        if existing_file.shape[0] > 0:
            existing_path = os.path.relpath(existing_file["folder"].iloc[0], target_folder)
            existing_path = os.path.join(existing_path, existing_file["name"].iloc[0])
            print("File", relative_path, "already exists in", existing_path)
        else:
            file_manager.copy_file(file_path, target_folder, relative_path)

    file_manager.display_stats()


def merge_folders(folder_src, folder_dst):
    """
    For each file in folder_src/** : copy it to folder_dst
    If file exits in folder_dst and is different : display a warning
    """
    folder_src = os.path.abspath(folder_src)
    folder_dst = os.path.abspath(folder_dst)
    file_manager = io_wrappers.FileManager()

    print(f"WILL DELETE duplicated files in {folder_src}.")
    if input("Please confirm (y/[N]) ").lower() != "y":
        return

    for root, dirs, files in os.walk(folder_src):
        relative_folder = os.path.relpath(root, folder_src)
        for basename in files:
            source_path = os.path.join(root, basename)
            relative_path = os.path.join(relative_folder, basename)

            if os.path.getsize(source_path) == 0:
                # print("  empty file", source_path)
                # os.remove(source_path)
                pass
            elif io_wrappers.is_hidden_system_file(basename):
                # print("  temp file", source_path)
                # os.remove(source_path)
                pass
            else:
                # print(relative_path)
                file_manager.copy_file(source_path, folder_dst, relative_path)

    file_manager.display_stats()


def delete_duplicated_files(folder: str):
    """
    Keep files with:
    - the shortest folder : longest folder often have useless names like '_backup'
    - the longest name : keep maximum information
    """
    db_cache_manager = DbCacheManager(folder)
    keys, df_duplicated = db_cache_manager.find_duplicated_files()

    df_duplicated['name_len'] = -df_duplicated['name'].map(len)
    df_duplicated['folder_len'] = df_duplicated['folder'].map(len)

    trash_folder = os.getenv("TRASH_FOLDER", os.path.join(folder, "duplicated"))

    for files_info, sub_df in df_duplicated.groupby(keys):
        sub_df = sub_df.sort_values(['folder_len', 'name_len'])
        for idx, row in sub_df.iloc[1:, :].iterrows():
            source_path = os.path.join(folder, row['folder'], row['name'])
            target_path = os.path.join(trash_folder, row['folder'], row['name'])
            os.makedirs(os.path.dirname(target_path), exist_ok=True)

            print("move", source_path, target_path)
            shutil.move(source_path, target_path)

    print('duplicated :')
    print(df_duplicated)


def main():
    def display_help():
        print("Expecting argument in", set(args_2_func.keys()))

    load_dotenv(dotenv_path=Path.cwd() / '.env')
    args_2_func = {
        func.__name__: func
        for func in [copy_recursive, merge_folders, delete_duplicated_files]
    }

    if len(sys.argv) < 2:
        display_help()
        exit(0)

    func_name = sys.argv[1].lower()
    func = args_2_func.get(func_name, display_help)
    func(*sys.argv[2:])


if __name__ == '__main__':
    main()
