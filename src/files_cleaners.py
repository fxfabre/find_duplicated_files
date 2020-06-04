#!/usr/bin/env python3

import os
import shutil

from src.db_cache.db_cache_manager import DbCacheManager
from src.io_files import io_wrappers
from src.io_files import hash_functions


def copy_recursive(folder_src: str, folder_dst: str):
    """
    Use the database to find each file from folder_src in folder_dst/**
    ie : file folder_src/Folder_A/File_01 won't be cp to folder_dst
        if File_01 exists in folder_dst/Folder_B/Folder_C
    """
    folder_src = os.path.abspath(folder_src)
    folder_dst = os.path.abspath(folder_dst)
    src_hash_gen = hash_functions.FileHashManager(folder_src)
    dst_hash_gen = hash_functions.FileHashManager(folder_dst)

    db_cache = DbCacheManager(dst_hash_gen)
    file_manager = io_wrappers.FileManager()

    for root, file_name in io_wrappers.iter_on_files(folder_src):
        file_path = os.path.join(root, file_name)
        relative_folder = os.path.relpath(root, folder_src)
        relative_path = os.path.join(relative_folder, file_name)

        if os.path.getsize(file_path) < 10 * 1024:
            file_manager.copy_file(file_path, folder_dst, relative_path)
            continue

        folder_doublon, name_doublon = db_cache.find_file(file_path, src_hash_gen)

        if name_doublon:
            existing_path = os.path.join(folder_doublon, name_doublon)
            print("File", relative_path, "already exists in", existing_path)
        else:
            file_manager.copy_file(file_path, folder_dst, relative_path)
            # TODO : update cache

    file_manager.display_stats()


def merge_folders(folder_src, folder_dst):
    """
    For each file in folder_src/** : copy it to folder_dst
    If file exits in folder_dst and is different : display a warning
    Don't need any database
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
