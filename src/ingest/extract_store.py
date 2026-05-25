import zipfile
import os
import shutil

ZIP_FILE_PATH = "./data/raw/store_data.zip"
EXTRACT_PATH = "./data/raw/store_extracted"
TARGET_PATH = "./data/raw/"

def extract_seoul_gyeonggi_store_data():
    if not os.path.exists(ZIP_FILE_PATH):
        print(f"오류: {ZIP_FILE_PATH} 파일이 없습니다.")
        print("공공데이터포털에서 ZIP 파일을 다운로드하여 해당 경로에 넣어주세요.")
        return
    print("1. 상권 데이터 ZIP 파일 압축 해제 중...")
    os.makedirs(EXTRACT_PATH, exist_ok=True)
    with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACT_PATH)
    print("2. 수도권(서울, 경기) 데이터만 추출하여 이동 중...")
    
    for file_name in os.listdir(EXTRACT_PATH):
        if '서울' in file_name or '경기' in file_name:
            source = os.path.join(EXTRACT_PATH, file_name)
            destination = os.path.join(TARGET_PATH, file_name)
            shutil.copy2(source, destination)
            print(f" -> 추출 완료: {file_name}")

    print("3. 다 쓴 임시 압축 폴더 삭제(정리) 중...")
    shutil.rmtree(EXTRACT_PATH)
    
    print(" 상권 데이터 수집 및 추출이 완료되었습니다!")

if __name__ == "__main__":
    extract_seoul_gyeonggi_store_data()