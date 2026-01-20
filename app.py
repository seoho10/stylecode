import snowflake.connector
import pandas as pd
import os

def update_dashboard():
    print("🚀 Snowflake 데이터 추출 시작...")
    
    try:
        # 1. 깃허브 Secrets에 저장한 환경 변수들을 불러와 연결합니다.
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )

        # 2. 실행할 쿼리를 작성합니다.
        # ★중요: 'YOUR_TABLE_NAME' 부분을 실제 Snowflake 테이블 이름으로 바꾸셔야 합니다!
        sql = "SELECT * FROM PRCS.DW_SALE LIMIT 100" 
        
        # 3. 데이터를 가져와서 분석하기 좋게 만듭니다.
        df = pd.read_sql(sql, conn)
        
        # 4. 파일 저장 경로 수정 (현재 폴더에 바로 저장)
        # 깃허브 구조에 맞춰 'frontend/'를 제거했습니다.
        output_path = 'data.json'
        df.to_json(output_path, orient='records', force_ascii=False)
        
        print(f"✅ 성공: {output_path} 파일이 생성되었습니다!")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        
    finally:
        # 연결 종료
        if 'conn' in locals():
            conn.close()
            print("🔌 Snowflake 연결 종료")

if __name__ == "__main__":
    update_dashboard()