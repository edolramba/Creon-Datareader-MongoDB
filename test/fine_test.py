import sys
sys.path.append('C:/Dev/Creon-Datareader')  # MongoDBHandler가 있는 경로를 추가

from PyQt5.QtWidgets import QApplication
from PyQt5.QtTest import QTest
from PyQt5.QtCore import Qt
from creon_datareader import MainWindow

def test_gui():
    app = QApplication(sys.argv)
    main = MainWindow()
    main.show()

    # 데이터베이스 연결 테스트
    assert main.db_handler._client is not None, "Database connection should be established."

    # API 호출 테스트
    code_list = main.objCodeMgr.get_code_list(1)  # 코스피 종목 코드 가져오기
    assert len(code_list) > 0, "Should retrieve non-empty code list from Creon."

    # MongoDB 저장 테스트
    test_data = {'code': '000660', 'date': 20210430, 'open': 90000, 'close': 91000}
    inserted_id = main.db_handler.insert_item(test_data, 'creon_data', 'test_collection')
    assert inserted_id is not None, "Should insert data into MongoDB."

    # MongoDB 조회 테스트
    retrieved_data = main.db_handler.find_item({'code': '000660'}, 'creon_data', 'test_collection')
    assert retrieved_data['date'] == 20210430, "Retrieved data should match the inserted data."

    # GUI 반응 테스트
    QTest.mouseClick(main.pushButton_4, Qt.LeftButton)  # 전체 다운로드 버튼 클릭
    assert main.update_status_msg != '', "Status message should be updated after download."

    # 종료 전에 테스트 컬렉션 정리
    main.db_handler.delete_items({}, 'creon_data', 'test_collection')

    print("All tests passed!")
    sys.exit()

if __name__ == "__main__":
    test_gui()
