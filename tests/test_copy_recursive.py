#!/usr/bin/env python3
import os
import shutil
from tempfile import mkdtemp
from unittest import mock
from unittest import TestCase

from sqlalchemy import create_engine

from src.files_cleaners import copy_recursive


class TestCopyResursive(TestCase):
    def setUp(self) -> None:
        super(TestCopyResursive, self).setUp()
        self.engine = create_engine("sqlite://")  # in-memory database
        self.test_folder = mkdtemp()
        self.folder_src = os.path.join(self.test_folder, "folder_src")
        self.folder_dst = os.path.join(self.test_folder, "folder_dst")

        patch_func = "src.db_cache.db_cache_manager.create_engine"
        self.patch_query = mock.patch(patch_func, lambda _: self.engine)
        self.patch_query.start()
        self.addCleanup(self.patch_query.stop)

    def tearDown(self) -> None:
        super(TestCopyResursive, self).tearDown()
        self.engine.dispose()
        shutil.rmtree(self.test_folder)

    def create_file_src(self):
        subfolder1 = os.path.join(self.folder_src, "subfolder1")
        subfolder2 = os.path.join(self.folder_src, "subfolder2")
        os.makedirs(subfolder1)
        os.makedirs(subfolder2)

        with open(os.path.join(subfolder1, "file1.txt"), "w") as f:
            f.write("file1" * 50000)
        with open(os.path.join(subfolder1, "file2.txt"), "w") as f:
            f.write("file2" * 50000)
        with open(os.path.join(subfolder1, "file3.txt"), "w") as f:
            f.write("file3_version1" * 50000)
        with open(os.path.join(subfolder2, "file4.txt"), "w") as f:
            f.write("file4" * 50000)

        return self.list_files_in_folder(self.folder_src)

    def create_file_dst(self):
        subfolder1 = os.path.join(self.folder_dst, "subfolder1")
        subfolder3 = os.path.join(self.folder_dst, "subfolder3")
        os.makedirs(subfolder1)
        os.makedirs(subfolder3)

        with open(os.path.join(subfolder1, "file1.txt"), "w") as f:
            f.write("file1" * 50000)
        with open(os.path.join(subfolder1, "file3.txt"), "w") as f:
            f.write("file3_version2" * 50000)
        with open(os.path.join(subfolder3, "file2.txt"), "w") as f:
            f.write("file2" * 50000)
        with open(os.path.join(subfolder3, "file5.txt"), "w") as f:
            f.write("file5" * 50000)

        return self.list_files_in_folder(self.folder_dst)

    def list_files_in_folder(self, folder_path):
        idx_start = len(folder_path) + 1
        return {
            os.path.join(root, name)[idx_start:]
            for root, dirs, files in os.walk(folder_path)
            for name in files
        }

    def test_copy_files(self):
        files_src = self.create_file_src()
        files_dst = self.create_file_dst()
        self.assertEqual(4, len(files_src))
        self.assertEqual(4, len(files_dst))

        copy_recursive(self.folder_src, self.folder_dst)

        all_files = self.list_files_in_folder(self.folder_dst)
        expected_files = {
            "subfolder1/file1.txt",
            "subfolder1/file3.txt",
            "subfolder1/file3_1.txt",
            "subfolder2/file4.txt",
            "subfolder3/file2.txt",
            "subfolder3/file5.txt",
        }
        self.assertEqual(all_files, expected_files, all_files)
