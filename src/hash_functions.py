#!/usr/bin/env python3

import os
import hashlib

SIZE_READ = 1024 * 1024

HASH_FUNCTIONS = {'md5': hashlib.md5, 'sha256': hashlib.sha256}


def get_hashs_for_file(file_path):
    hashs = {func_name: func() for func_name, func in HASH_FUNCTIONS.items()}
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(SIZE_READ), b""):
            for hash_name in hashs.keys():
                hashs[hash_name].update(chunk)

    return dict(
        {hash_name: hash_func.hexdigest() for hash_name, hash_func in hashs.items()},
        size=os.path.getsize(file_path)
    )
