#!/usr/bin/env python3
import os
import shutil
from tempfile import mkdtemp
from typing import Set
from unittest import mock
from unittest import TestCase

from freezegun import freeze_time
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

        self.subfolder1 = os.path.join(self.folder_src, "subfolder1")
        self.subfolder2 = os.path.join(self.folder_src, "subfolder2")
        os.makedirs(self.subfolder1)
        os.makedirs(self.subfolder2)

        self.create_folder_src()
        self.files_dst_before_cp = self.create_file_dst()
        self.assertEqual(4, len(self.files_dst_before_cp))

    def tearDown(self) -> None:
        super(TestCopyResursive, self).tearDown()
        self.engine.dispose()
        shutil.rmtree(self.test_folder)

    @classmethod
    def list_files_in_folder(cls, folder_path) -> Set[str]:
        idx_start = len(folder_path) + 1
        return {
            os.path.join(root, name)[idx_start:]
            for root, dirs, files in os.walk(folder_path)
            for name in files
        }

    def create_folder_src(self):

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

        return {
            "subfolder1/file1.txt",
            "subfolder1/file3.txt",
            "subfolder3/file2.txt",
            "subfolder3/file5.txt",
        }

    def test_copy_same_file(self):
        with open(os.path.join(self.subfolder1, "file1.txt"), "w") as f:
            f.write("file1" * 50000)

        copy_recursive(self.folder_src, self.folder_dst)

        all_files = self.list_files_in_folder(self.folder_dst)
        self.assertEqual(all_files, self.files_dst_before_cp, all_files)

    def test_copy_same_file_different_folder(self):
        with open(os.path.join(self.subfolder1, "file2.txt"), "w") as f:
            f.write("file2" * 50000)

        copy_recursive(self.folder_src, self.folder_dst)

        all_files = self.list_files_in_folder(self.folder_dst)
        self.assertEqual(all_files, self.files_dst_before_cp, all_files)

    @freeze_time("2020-06-18")
    def test_copy_same_name_different_content__date(self):
        with open(os.path.join(self.subfolder1, "file3.txt"), "w") as f:
            f.write("file3_version1" * 50000)

        copy_recursive(self.folder_src, self.folder_dst)

        all_files = self.list_files_in_folder(self.folder_dst)
        expected = self.files_dst_before_cp | {"subfolder1/file3_20200618.txt"}
        self.assertEqual(all_files, expected, all_files)

    @freeze_time("2020-06-19")
    def test_copy_same_name_different_content__date_1(self):
        with open(os.path.join(self.subfolder1, "file3.txt"), "w") as f:
            f.write("file3_version1" * 50000)

        original_file_3 = os.path.join(self.folder_dst, "subfolder1", "file3.txt")
        copy_date = os.path.join(self.folder_dst, "subfolder1", "file3_20200619.txt")
        shutil.copy2(original_file_3, copy_date)

        copy_recursive(self.folder_src, self.folder_dst)

        after_cp = self.list_files_in_folder(self.folder_dst)
        expected = self.files_dst_before_cp | {
            "subfolder1/file3_20200619.txt",
            "subfolder1/file3_20200619_1.txt",
        }
        self.assertEqual(after_cp, expected, after_cp)

    @freeze_time("2020-06-20")
    def test_copy_same_name_different_content__date_2(self):
        with open(os.path.join(self.subfolder1, "file3.txt"), "w") as f:
            f.write("file3_version1" * 50000)

        original_file_3 = os.path.join(self.folder_dst, "subfolder1", "file3.txt")
        copy_date = os.path.join(self.folder_dst, "subfolder1", "file3_20200620.txt")
        shutil.copy2(original_file_3, copy_date)

        copy_date = os.path.join(self.folder_dst, "subfolder1", "file3_20200620_1.txt")
        shutil.copy2(original_file_3, copy_date)
        before_cp = self.list_files_in_folder(self.folder_dst)

        copy_recursive(self.folder_src, self.folder_dst)

        after_cp = self.list_files_in_folder(self.folder_dst)
        expected = before_cp | {"subfolder1/file3_20200620_2.txt"}
        self.assertEqual(after_cp, expected, after_cp)

    def test_copy_new_file(self):
        with open(os.path.join(self.subfolder2, "file4.txt"), "w") as f:
            f.write("file4" * 50000)

        before_cp = self.list_files_in_folder(self.folder_dst)

        copy_recursive(self.folder_src, self.folder_dst)

        after_cp = self.list_files_in_folder(self.folder_dst)
        expected = before_cp | {"subfolder2/file4.txt"}
        self.assertEqual(after_cp, expected, after_cp)
