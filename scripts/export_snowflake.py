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
    account = must_env("SF_ACCOUNT")
    user = must_env("SF_USER")
    password = must_env("SF_PASSWORD")
    warehouse = must_env("SF_WAREHOUSE")
    database = must_env("SF_DATABASE")
    schema = must_env("SF_SCHEMA")
    role = os.getenv("SF_ROLE")
    brd_cd = os.getenv("BRD_CD", "X")

    end_dt = datetime.utcnow().date()
    start_dt = end_dt - timedelta(days=7)

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
