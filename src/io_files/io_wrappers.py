#!/usr/bin/env python3
import filecmp
import os
import shutil


def is_hidden_system_file(basename: str) -> bool:
    # if basename.startswith('._'):
    #     return True
    # if basename.startswith('~$'):
    #     return True

    return basename.lower() in [".ds_store", "thumbs.db", "desktop.ini"]


def files_equals(source_path, target_path):
    return filecmp.cmp(source_path, target_path, shallow=False)


def remove_folder(root: str):
    print(f"Remove empty folder {root}")
    os.rmdir(root)  # will fail if folder not empty


def iter_on_files(folder: str):
    for root, dirs, files in os.walk(folder, topdown=False):
        print(root)

        for file_name in files:
            if not is_hidden_system_file(file_name):
                yield root, file_name


class FileManager:
    def __init__(self):
        super(FileManager, self).__init__()
        self._moved = 0
        self._duplicated = 0

    def copy_file(self, source_path, folder_dst, relative_path):
        target_path = os.path.join(folder_dst, relative_path)

        if os.path.exists(target_path):
            if files_equals(source_path, target_path):
                self._duplicated += 1
            else:
                name, ext = os.path.splitext(relative_path)
                relative_path = f"{name}_1{ext}"
                return self.copy_file(source_path, folder_dst, relative_path)
        else:
            print(f"mv {relative_path:<150} -> {folder_dst}")
            self._moved += 1
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            shutil.copy(source_path, target_path)

    def display_stats(self):
        print("Summary :")
        print("  Moved      :", self._moved)
        print("  Duplicates :", self._duplicated)
