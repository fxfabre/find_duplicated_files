# find_duplicated_files

TODO : see
- https://github.com/pixelb/fslint
- https://github.com/arsenetar/dupeguru

## Use case
Python script to avoid duplicated files, when we have files on :
- local hard drive
- USB stick
- external hard drive
- smartphone
- ...

The script can :
- find duplicated files in a folder
- copy recursively files from folder_A to folder_B, ignoring files that already in folder_B (or any sub folder)

How does it works ?

Use case = copy recursively files from folder_A to folder_B
- The script first create a database with (file path, size, md5, sha1) for all files in folder_B
- Then for all files in folder_A : compare (size, md5, sha1) to the database. If it finds a match in the database, the file is skipped. Otherwise copy to folder_B

## Set up
**python dependencies :**
```bash
pip install -r requirements.txt
```

**Environment variables :**

Create a file named `.env` with the following variables, and change the values:
- POSTGRES_PASSWORD : the password to create / access the postgres db
- TRASH_FOLDER : where to move the duplicated files

example :
```
POSTGRES_PASSWORD=your_postgres_password
TRASH_FOLDER='/tmp/trash_folder'
```

**Postgres database :**
```bash
docker-compose up -d
```


## Run
```bash
python main.py
```


## TODO
- Use argparse
- Use logger
- Use sqlalchemy to manage database structure / queries
