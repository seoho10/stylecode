import os
import json
from datetime import datetime, timedelta
import snowflake.connector
from dotenv import load_dotenv

# 현재 스크립트 위치를 기준으로 상위 폴더(frontend)에 있는 .env 파일을 로드합니다.
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)

def must_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        # 에러 발생 시 시도한 .env 경로를 함께 출력하여 디버깅을 돕습니다.
        raise RuntimeError(f"Missing env: {name} (Checked path: {os.path.abspath(env_path)})")
    return v

def main():
    # 환경 변수 로드
    account = must_env("SF_ACCOUNT")
    user = must_env("SF_USER")
    password = must_env("SF_PASSWORD")
    warehouse = must_env("SF_WAREHOUSE")
    database = must_env("SF_DATABASE")
    schema = must_env("SF_SCHEMA")

    role = os.getenv("SF_ROLE")
    # 브랜드 코드가 설정되어 있지 않으면 기본값 'X'를 사용합니다.
    brd_cd = os.getenv("BRD_CD", "X")

    # 최근 30일 데이터 추출 범위 설정
    end_dt = datetime.utcnow().date()
    start_dt = end_dt - timedelta(days=30)

    conn_args = {
        "account": account,
        "user": user,
        "password": password,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
    }
    if role:
        conn_args["role"] = role

    # Snowflake 연결
    conn = snowflake.connector.connect(**conn_args)
    cur = conn.cursor()

    try:
        # 1. 판매 데이터 추출 쿼리 (JOIN 및 필터 조건 적용)
        sql = """
        SELECT 
            A.SALE_DT,
            A.BRD_CD,
            A.PART_CD,
            CASE 
                WHEN A.ONLINE_YN = 'Y' THEN '온라인'
                WHEN A.ONLINE_YN = 'N' THEN '오프라인'
                ELSE '기타'
            END AS ANLYS_ON_OFF_CLS_NM,
            SUM(A.SALE_AMT) AS ALL_AMT,
            SUM(A.QTY) AS ALL_QTY,
            0 AS CID_AMT,
            0 AS CID_QTY,
            0 AS CID_CNT
        FROM PRCS.DW_SALE A
        INNER JOIN PRCS.DB_SHOP B
            ON A.SHOP_ID = B.SHOP_ID
            AND A.BRD_CD = B.BRD_CD
        WHERE A.BRD_CD = %s
          AND A.SALE_DT BETWEEN %s AND %s
          AND A.PART_CD IS NOT NULL
          AND B.ANAL_CNTRY_NM = '한국'
          AND B.MNG_TYPE_NM = '위탁'
          AND B.ANAL_DIST_TYPE_NM IN ('백화점', '직영점', '대리점', '온라인')
        GROUP BY 
            A.SALE_DT, 
            A.BRD_CD, 
            A.PART_CD,
            A.ONLINE_YN
        ORDER BY A.SALE_DT, A.BRD_CD, A.PART_CD, ANLYS_ON_OFF_CLS_NM
        """
        cur.execute(sql, (brd_cd, str(start_dt), str(end_dt)))

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        raw_data = [dict(zip(cols, r)) for r in rows]

        # 데이터 전처리 (날짜 형식 및 Null 처리, '기타' 제외)
        data = []
        for item in raw_data:
            # ANLYS_ON_OFF_CLS_NM이 '기타'인 경우 제외
            if item.get("ANLYS_ON_OFF_CLS_NM") == '기타':
                continue
            
            v = item.get("SALE_DT")
            if hasattr(v, "isoformat"):
                item["SALE_DT"] = v.isoformat()
            
            for num_field in ["ALL_AMT", "ALL_QTY", "CID_AMT", "CID_QTY", "CID_CNT"]:
                if item.get(num_field) is None:
                    item[num_field] = 0
            
            data.append(item)

        # 데이터 저장 폴더 생성 (scripts와 같은 레벨의 data 폴더)
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        # 판매 데이터 저장
        out_path = os.path.join(data_dir, "sales_daily.json")
        payload = {
            "generated_at_utc": datetime.utcnow().isoformat() + "Z",
            "range": {"start": str(start_dt), "end": str(end_dt)},
            "brd_cd": brd_cd,
            "data": data,
        }

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        print(f"OK: wrote {out_path} rows={len(data)}")

        # 2. 브랜드 목록 추출 쿼리
        brand_sql = """
        SELECT DISTINCT BRD_CD
        FROM PRCS.DB_SHOP
        WHERE BRD_CD IN ('M','I','ST','V','X')
        ORDER BY BRD_CD
        """
        cur.execute(brand_sql)
        brand_rows = cur.fetchall()
        brands = [r[0] for r in brand_rows]

        # 브랜드 목록 저장
        brand_out_path = os.path.join(data_dir, "brands.json")
        brand_payload = {
            "generated_at_utc": datetime.utcnow().isoformat() + "Z",
            "brands": brands
        }

        with open(brand_out_path, "w", encoding="utf-8") as f:
            json.dump(brand_payload, f, ensure_ascii=False, indent=2)

        print(f"OK: wrote {brand_out_path} brands={len(brands)}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()