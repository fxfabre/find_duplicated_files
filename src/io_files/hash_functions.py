#!/usr/bin/env python3
import hashlib
import os
from typing import Dict
from typing import Iterable

from src.io_files import io_wrappers

SIZE_READ = 1024 * 1024

HASH_FUNCTIONS = {"md5": hashlib.md5, "sha256": hashlib.sha256}


class FileHashManager:
    def __init__(self, ref_folder):
        super(FileHashManager, self).__init__()
        self.data_folder = ref_folder

    def generate_file_records(self) -> Iterable[Dict]:
        for root, file_name in io_wrappers.iter_on_files(self.data_folder):
            try:
                yield self.create_record(root, file_name)
            except FileNotFoundError as e:
                print(e)
                print(root, file_name)

    def create_record(self, folder: str, file_name: str) -> Dict:
        file_path = os.path.join(folder, file_name)
        hash_info = self.get_hashs_for_file(file_path)
        return dict(
            {"name": file_name, "folder": os.path.relpath(folder, self.data_folder)},
            **hash_info
        )

    @classmethod
    def get_hashs_for_file(cls, file_path):
        return get_hashs_for_file(file_path)


def get_hashs_for_file(file_path):
    hashs = {func_name: func() for func_name, func in HASH_FUNCTIONS.items()}
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(SIZE_READ), b""):
            for hash_name in hashs.keys():
                hashs[hash_name].update(chunk)

    return dict(
        {hash_name: hash_func.hexdigest() for hash_name, hash_func in hashs.items()},
        size=os.path.getsize(file_path),
    )
