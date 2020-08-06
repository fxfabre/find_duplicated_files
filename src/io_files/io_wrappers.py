#!/usr/bin/env python3
import filecmp
import os
import shutil
from datetime import datetime


def is_hidden_system_file(folder: str, basename: str) -> bool:
    if basename.lower() in [".ds_store", "thumbs.db", "desktop.ini", ".picasa.ini"]:
        return True

    if basename.startswith("._") or basename.startswith("~$"):
        file_path = os.path.join(folder, basename)
        return os.path.getsize(file_path) <= 6 * 1024

    return False


def files_equals(source_path, target_path):
    return filecmp.cmp(source_path, target_path, shallow=False)


def remove_folder(root: str):
    print(f"Remove empty folder {root}")
    os.rmdir(root)  # will fail if folder not empty


def iter_on_files(folder: str):
    for root, dirs, files in os.walk(folder, topdown=False):
        for file_name in files:
            if not is_hidden_system_file(root, file_name):
                yield root, file_name


class FileManager:
    def __init__(self):
        super(FileManager, self).__init__()
        self._copied = 0
        self._duplicated = []

    def copy_file(self, source_path, folder_dst, relative_path):
        target_path = os.path.join(folder_dst, relative_path)
        if os.path.exists(target_path) and files_equals(source_path, target_path):
            return

        date = None
        suffix = None
        name, ext = os.path.splitext(target_path)

        while os.path.exists(target_path):
            if files_equals(source_path, target_path):
                self._duplicated.append((source_path, target_path))
                return

            if date is None:
                date = datetime.now().strftime("%Y%m%d")
                target_path = f"{name}_{date}{ext}"
            elif suffix is None:
                suffix = 1
                target_path = f"{name}_{date}_{suffix}{ext}"
            else:
                suffix += 1
                target_path = f"{name}_{date}_{suffix}{ext}"

        print(f"cp {relative_path:<130} -> {folder_dst}")
        self._copied += 1
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        shutil.copy2(source_path, target_path)

    def display_stats(self):
        print("Summary :")
        print("  Copy       :", self._copied)
        print("  Duplicates :", len(self._duplicated))
