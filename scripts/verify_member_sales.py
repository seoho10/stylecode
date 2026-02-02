"""data/sales_daily.json에서 회원/비회원 매출 집계 확인 (대시보드 회원 비중 검증용)"""
import os
import json

def main():
    data_path = os.path.join(os.path.dirname(__file__), "..", "data", "sales_daily.json")
    if not os.path.exists(data_path):
        print("파일 없음:", data_path)
        return
    with open(data_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    data = payload.get("data") or []
    if not data:
        print("data 배열이 비어 있습니다.")
        return

    member_amt = member_qty = non_amt = non_qty = 0
    for row in data:
        cid = row.get("CUST_ID")
        has_cust = cid is not None and str(cid).strip() != ""
        amt = int(row.get("ALL_AMT") or 0)
        qty = int(row.get("ALL_QTY") or 0)
        if has_cust:
            member_amt += amt
            member_qty += qty
        else:
            non_amt += amt
            non_qty += qty

    total_amt = member_amt + non_amt
    total_qty = member_qty + non_qty
    print("=== 회원/비회원 매출 확인 ===")
    print("총 행 수:", len(data))
    print("회원 매출:", member_amt, "원 / 회원 수량:", member_qty)
    print("비회원 매출:", non_amt, "원 / 비회원 수량:", non_qty)
    print("전체 매출:", total_amt, "원 / 전체 수량:", total_qty)
    if total_amt:
        print("회원 비중(매출):", round(100 * member_amt / total_amt, 1), "%")
    print("회원 매출이 0이 아니면 대시보드 회원 비중 차트에 반영됩니다.")

if __name__ == "__main__":
    main()
