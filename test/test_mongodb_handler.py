import sys
sys.path.append('C:/Dev/Creon-Datareader')  # MongoDBHandler가 있는 경로를 추가
import unittest
from util.MongoDBHandler import MongoDBHandler
# coding=utf-8
import sys
import pandas as pd


class TestMongoDBHandler(unittest.TestCase):
    
    def setUp(self):
        """테스트 전에 데이터베이스 연결을 설정합니다."""
        self.db_handler = MongoDBHandler()
        self.db_name = "test_db"
        self.collection_name = "test_collection"
        # 테스트 컬렉션을 비웁니다.
        self.db_handler._client[self.db_name][self.collection_name].delete_many({})
        
    def test_abc(self):
        # MongoDB에서 종목 코드 및 갱신 날짜를 포함한 데이터를 가져옵니다.
        db_data = self.db_handler.find_items({}, 'creon_data', 'stock_data', projection={'code': 1, '갱신날짜': 1, '_id': 0})
        if not db_data:
            print("데이터베이스에서 데이터를 가져오지 못했습니다.")
        else:
            db_code_list = [item['code'] for item in db_data if 'code' in item]
            db_name_list = [self.objCodeMgr.get_code_name(code) for code in db_code_list]
            db_latest_list = [item.get('갱신날짜', None) for item in db_data]  # None을 기본값으로 사용하여 '갱신날짜'가 없는 경우에 대비

            # 종목 코드, 이름, 갱신 날짜를 포함하는 DataFrame 생성
            self.db_code_df = pd.DataFrame({
                '종목코드': db_code_list,
                '종목명': db_name_list,
                '갱신날짜': db_latest_list
            })

if __name__ == '__main__':
    unittest.main()
