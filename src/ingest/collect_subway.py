import requests
import pandas as pd
import os
import time

API_KEY = "525a6747667379793839766875654d"

START_MONTH = 202501
END_MONTH = 202512

def collect_subway_data():
    os.makedirs("./data/raw", exist_ok=True)
    for month in range(START_MONTH, END_MONTH + 1):
        if int(str(month)[-2:]) > 12 or int(str(month)[-2:]) == 0:
            continue
        print(f"{month} 지하철 승하차 데이터 수집 중...")
        url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/CardSubwayTime/1/999/{month}"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'CardSubwayTime' in data:
                    rows = data['CardSubwayTime']['row']
                    df = pd.DataFrame(rows)
                    
                    file_path = f"./data/raw/subway_time_{month}.csv"
                    df.to_csv(file_path, index=False, encoding='utf-8-sig')
                    print(f" -> 성공: {file_path} (총 {len(df)}건)")
                else:
                    print(f" -> 데이터가 없습니다: {data.get('RESULT', '알 수 없는 에러')}")
            else:
                print(f" -> API 호출 실패 (상태 코드: {response.status_code})")
                
        except Exception as e:
            print(f" -> 에러 발생: {e}")
        
        time.sleep(1)
if __name__ == "__main__":
    collect_subway_data()