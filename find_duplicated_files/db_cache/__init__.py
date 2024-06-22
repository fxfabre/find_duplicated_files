#!/usr/bin/env python3
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Folders(Base):
    __tablename__ = "folders"

    folder_id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    folder_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False)

    def __str__(self):
        return f"{self.folder_id} : folder {self.folder_name}, table {self.table_name}"
