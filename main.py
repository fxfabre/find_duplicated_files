#!/usr/bin/env python3

import sys
from dotenv import load_dotenv
from pathlib import Path

from src.files_cleaners import copy_recursive


def main():
    def display_help():
        print("Expecting argument in", set(args_2_func.keys()))

    load_dotenv(dotenv_path=Path.cwd() / '.env')
    args_2_func = {
        func.__name__: func
        for func in [copy_recursive]
    }

    if len(sys.argv) < 2:
        display_help()
        exit(0)

    func_name = sys.argv[1].lower()
    func = args_2_func.get(func_name, display_help)
    func(*sys.argv[2:])


if __name__ == '__main__':
    main()
