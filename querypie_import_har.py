#!/usr/bin/env python3
"""HAR 에서 QueryPie 세션 페이로드를 추출해 querypie-open-<커넥션>.b64 로 저장.

**보통은 이 도구가 필요 없다.** 조회 도구와 프록시는 커넥션 이름만 있으면 그 클러스터의
엔드포인트로 페이로드를 그 자리에서 조립하므로(querypie_query.resolve_open_payload) 등록
자체가 없다. 특정 노드를 굳이 박제해야 하면 `querypie_conn.py add <이름> --match <노드>` 로
API 만으로 만들 수 있다. 이 도구는 그 조립마저 실패하는 커넥션을 만났을 때의 대비책이다.

준비:
  1. QueryPie 웹 UI 에서 그 인스턴스의 DB 를 열어 아무 쿼리나 한 번 실행한다.
  2. 개발자도구 Network 를 HAR 로 저장한다 (SessionService/open 요청이 포함돼야 한다).

사용:
  python querypie_import_har.py <커넥션이름> <HAR 경로> [--force]
  python querypie_import_har.py shop ~/Downloads/shop.har

저장 후 `querypie_conn.py map <커넥션이름>` 을 돌리면 그 커넥션의 database 들이
querypie-conn-map.json 에 등록되어 `--db <database>` 만으로 커넥션이 자동 선택된다.

페이로드에는 connection 메타만 담기고 인증 토큰은 없다 (인증은 쿠키 파일 담당).
식별자(connectionUuid)는 stdout 에 출력하지 않는다.
"""
import argparse
import base64
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")

import querypie_query as qp


def first_frame(entry):
    """gRPC-web text 요청 본문에서 첫 데이터 프레임(페이로드) 추출."""
    raw = qp.b64d(entry["request"].get("postData", {}).get("text") or "")
    for flag, payload in qp.unframe(raw):
        if not flag & 0x80:
            return payload
    return None


def extract_open_payload(har_path):
    """HAR 의 첫 SessionService/open 요청 페이로드를 그대로 반환 (없으면 None).

    connection 객체 조립이 복잡해 원본을 그대로 재생(replay)한다. 같은 커넥션에
    대한 세션 재확립이므로 유효하다.
    """
    with open(har_path, encoding="utf-8") as fp:
        har = json.load(fp)
    for entry in har["log"]["entries"]:
        if "SessionService/open" not in entry["request"]["url"]:
            continue
        payload = first_frame(entry)
        if payload:
            return payload
    return None


def main():
    ap = argparse.ArgumentParser(description="HAR -> QueryPie 세션 페이로드 저장")
    ap.add_argument("name", help="커넥션 이름 (클러스터나 인스턴스를 가리키는 짧은 이름)")
    ap.add_argument("har", help="SessionService/open 이 담긴 HAR 경로")
    ap.add_argument("--force", action="store_true", help="같은 이름의 페이로드가 이미 있어도 덮어쓰기")
    args = ap.parse_args()

    out = qp.OPEN_FILE_TMPL.format(name=args.name)
    if os.path.exists(out) and not args.force:
        sys.exit(f"이미 있습니다: {os.path.basename(out)} (덮어쓰려면 --force)")

    payload = extract_open_payload(args.har)
    if payload is None:
        sys.exit("HAR 에서 SessionService/open 요청을 찾지 못했습니다. "
                 "UI 에서 그 DB 로 쿼리를 한 번 실행한 뒤 다시 캡처하세요.")

    conn = qp.extract_field_raw(payload, 2)
    if not conn:
        sys.exit("페이로드에서 connectionUuid(#2) 를 찾지 못했습니다. open 요청이 맞는지 확인하세요.")

    qp.ensure_home()
    os.umask(0o077)
    with open(out, "w", encoding="utf-8") as fp:
        fp.write(base64.b64encode(payload).decode())
    print(f"저장 완료: {os.path.basename(out)} (payload={len(payload)}B)")
    print(f"다음: querypie_query.py --conn-name {args.name} --db mysql --sql \"SHOW DATABASES\" "
          f"로 database 목록을 확인해 querypie-conn-map.json 에 매핑을 추가하세요.")


if __name__ == "__main__":
    main()
