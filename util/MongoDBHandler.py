from common.importConfig import *
import pymongo
from pymongo import MongoClient
from pymongo.cursor import CursorType
import datetime
from datetime import date

class MongoDBHandler:
    
    def __init__(self):
        importConf = importConfig()
        host = importConf.select_section("MONGODB")["host"]
        port = importConf.select_section("MONGODB")["port"]
        self._client = MongoClient(host, int(port))

    def ensure_unique_index(self, db_name, collection_name, field_name):
        """Ensure a unique index on a specified field in a collection."""
        self.validate_params(db_name, collection_name)
        current_indexes = self._client[db_name][collection_name].index_information()
        if field_name not in current_indexes:  # This checks based on index name, adjust if needed
            self._client[db_name][collection_name].create_index([(field_name, pymongo.ASCENDING)], unique=True)

    def validate_params(self, db_name, collection_name):
        if not db_name or not collection_name:
            raise Exception("Database name and collection name must be provided.")
    
    def insert_item(self, data, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if not isinstance(data, dict):
            raise Exception("data type should be dict")
        return self._client[db_name][collection_name].insert_one(data).inserted_id

    def insert_items(self, datas, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if not isinstance(datas, list):
            raise Exception("datas type should be list")
        return self._client[db_name][collection_name].insert_many(datas).inserted_ids

    def find_item(self, condition=None, db_name=None, collection_name=None, sort=None, projection=None):
        """Finds a single item in the specified collection using an optional sort order."""
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        query_options = {"_id": False}
        if projection:
            query_options.update(projection)

        # If a sort is specified, apply it and retrieve the first document from the sorted results.
        if sort:
            cursor = self._client[db_name][collection_name].find(condition, query_options).sort(sort)
            try:
                return next(cursor)  # Return the first item from the cursor
            except StopIteration:
                return None  # Return None if the cursor is empty
        else:
            return self._client[db_name][collection_name].find_one(condition, query_options)

    def find_items(self, condition=None, db_name=None, collection_name=None, sort=None, projection=None, limit=None):
        """Finds multiple items with optional sorting and limits."""
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        find_options = {}
        
        if projection:
            find_options['projection'] = projection
        
        cursor = self._client[db_name][collection_name].find(condition, **find_options)
        
        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)

        return list(cursor)

    def find_items_distinct(self, condition=None, db_name=None, collection_name=None, distinct_col=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        return self._client[db_name][collection_name].find(condition, {"_id": False}, no_cursor_timeout=True, cursor_type=CursorType.EXHAUST).distinct(distinct_col)

    def find_items_id(self, condition=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        return self._client[db_name][collection_name].find(condition, {"_id": True}, no_cursor_timeout=True, cursor_type=CursorType.EXHAUST)

    def find_item_id(self, condition=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        return self._client[db_name][collection_name].find_one(condition, {"_id": True})

    def delete_items(self, condition=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict):
            raise Exception("Condition must be provided as dict")
        return self._client[db_name][collection_name].delete_many(condition)

    def update_items(self, condition=None, update_value=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        return self._client[db_name][collection_name].update_many(filter=condition, update=update_value)

    def update_item(self, condition=None, update_value=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        return self._client[db_name][collection_name].update_one(filter=condition, update=update_value)
    
    def aggregate(self, pipeline=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if pipeline is None or not isinstance(pipeline, list):
            raise Exception("Pipeline must be provided as a list")
        return self._client[db_name][collection_name].aggregate(pipeline)

    def text_search(self, text=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if text is None or not isinstance(text, str):
            raise Exception("Text must be provided")
        return self._client[db_name][collection_name].find({"$text": {"$search": text}})

    def upsert_item(self, condition=None, update_value=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        return self._client[db_name][collection_name].update_one(filter=condition, update=update_value, upsert=True)

    def upsert_items(self, condition=None, update_value=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        return self._client[db_name][collection_name].update_many(filter=condition, update=update_value, upsert=True)

    def validate_params(self, db_name, collection_name=None):
        if not db_name:
            raise Exception("Database name must be provided.")
        if collection_name is not None and not collection_name:
            raise Exception("Collection name must be provided when specified.")
    
    # MongoDB의 데이터베이스에 컬렉션이 있는지 확인
    def check_database_exists(self, db_name):
        """Check if the specified database has any collections."""
        self.validate_params(db_name)
        # 해당 데이터베이스에 컬렉션 리스트를 가져와서 확인
        return len(self._client[db_name].list_collection_names()) > 0