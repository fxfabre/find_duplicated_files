# find_duplicated_files

### Set up python virtual env:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Set up the environment variables
Create a file named `.env` with the following variables, and change the values:
- POSTGRES_PASSWORD : the password to create / access the postgres db
- FOLDER_DOCUMENTS : the path to the folder where you want to find duplicated files

example :
```
POSTGRES_PASSWORD=your_postgres_password
FOLDER_DOCUMENTS='/home/user/Documents/some_folder'
```

### Set up Postgres database
```bash
docker-compose up &
```

### Run
```bash
python main.py
```

And check result in postgres database
