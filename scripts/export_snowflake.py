import os
import json
from datetime import datetime, timedelta
import snowflake.connector

def must_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env: {name}")
    return v

def main():
    # 필수
    user = must_env("SF_USER")
    password = must_env("SF_PASSWORD")
    warehouse = must_env("SF_WAREHOUSE")
    database = must_env("SF_DATABASE")
    schema = must_env("SF_SCHEMA")

    # 선택: role
    role = os.getenv("SF_ROLE")

    # 핵심: app.snowflake.com 계정은 host를 직접 지정하는 게 안전
    # 예: cixxjbf.snowflake.com
    host = os.getenv("SF_HOST")

    # account는 환경마다 다르게 필요할 수 있어 optional로 둡니다
    account = os.getenv("SF_ACCOUNT")

    brd_cd = os.getenv("BRD_CD", "X")

    end_dt = datetime.utcnow().date()
    start_dt = end_dt - timedelta(days=7)

    conn_args = {
        "user": user,
        "password": password,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
    }

    if role:
        conn_args["role"] = role

    # host 또는 account 중 하나는 반드시 세팅
    if host:
        conn_args["host"] = host
    elif account:
        conn_args["account"] = account
    else:
        raise RuntimeError("Missing env: SF_HOST (recommended) or SF_ACCOUNT")

    conn = snowflake.connector.connect(**conn_args)
    cur = conn.cursor()

    try:
        sql = """
        SELECT SALE_DT, SUM(SALE_AMT) AS SALE_AMT
        FROM PRCS.DW_SALE
        WHERE BRD_CD = %s
          AND SALE_DT BETWEEN %s AND %s
        GROUP BY 1
        ORDER BY 1
        """
        cur.execute(sql, (brd_cd, str(start_dt), str(end_dt)))

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        data = [dict(zip(cols, r)) for r in rows]

        os.makedirs("data", exist_ok=True)
        out_path = os.path.join("data", "sales_daily.json")

        payload = {
            "generated_at_utc": datetime.utcnow().isoformat() + "Z",
            "range": {"start": str(start_dt), "end": str(end_dt)},
            "brd_cd": brd_cd,
            "data": data,
        }

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        print(f"OK: wrote {out_path} rows={len(data)}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
