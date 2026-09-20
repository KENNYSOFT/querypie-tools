#!/usr/bin/env python3
r"""QueryPie 커넥션 목록과 database 매핑 관리 도구.

조회 도구(querypie_query.py)와 프록시는 커넥션 이름만 있으면 그 클러스터의 엔드포인트로
세션 페이로드를 그 자리에서 조립한다(querypie_query.resolve_open_payload). 그래서 평소에는
등록 작업이 없고, 이 도구는 (1) 어떤 커넥션이 있는지 보고 (2) database 매핑을 채우는 데 쓴다.

사용:
  python querypie_conn.py list              # 클러스터와 커넥션 목록, 자동 선택 대상 표시
  python querypie_conn.py map               # 모든 클러스터의 database 매핑 갱신
  python querypie_conn.py map shop          # 한 클러스터만 갱신
  python querypie_conn.py add shop --match shop-01   # 특정 노드를 b64 로 박제 (대비책)

`add` 는 조립이 통하지 않는 커넥션을 위한 폴백이다. 저장해 두면 그 이름은 조립 대신 파일을
쓰게 되므로, 노드가 교체되면 그 파일도 다시 만들어야 한다.

식별자(connectionUuid 등)는 stdout 에 출력하지 않는다.
"""
import argparse
import base64
import json
import os
import secrets
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import querypie_query as qp  # noqa: E402  (경로 삽입 후 import)

# 조립과 목록 조회는 조회 도구와 공유한다 (프록시도 같은 구현을 쓴다).
list_clusters = qp.list_clusters
list_instances = qp.list_instances
pick_instance = qp.pick_instance
open_payload_for = qp.open_payload_for


def collect(insecure=False, window_id=""):
    """모든 클러스터의 커넥션을 [(클러스터명, 커넥션 uuid, 이름, 종류)] 로 모은다."""
    rows = []
    for _, cluster_uuid, cluster_name in list_clusters(insecure, window_id):
        for conn_uuid, name, kind in list_instances(cluster_uuid, insecure, window_id):
            rows.append((cluster_name, conn_uuid, name, kind))
    return rows


def fetch_databases(conn_uuid, open_payload, insecure=False, window_id=""):
    """그 커넥션의 database 목록을 받아온다 (매핑 갱신용)."""
    qp.session_open(open_payload, insecure, window_id)
    frames = qp.call("engine.user.once.dictionary.UserOnceDatabaseDictionaryService",
                     "GetDatabases", qp.f_str(1, conn_uuid), insecure, window_id=window_id)
    names = []
    for d in frames:
        for fn, entry in qp.iter_fields(d):
            if fn != 1:
                continue
            name = qp.find_field(qp.decode_raw(entry), 1, "str")
            if name:
                names.append(name)
    return names


# 어느 클러스터에나 있는 스키마라 매핑해 봐야 의미가 없다. 넣어 두면 마지막에 갱신한
# 클러스터가 이기므로, 그 이름으로 조회했을 때 엉뚱한 클러스터로 붙는다.
SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}


def dump_conn_map(mapping):
    """conn-map.json 을 커넥션별로 묶어 빈 줄로 구분해 쓴다 (사람이 읽는 파일이다)."""
    groups = {}
    for db, name in sorted(mapping.items()):
        groups.setdefault(name, []).append(db)
    lines = []
    for i, name in enumerate(sorted(groups)):
        if i:
            lines.append("")
        lines.extend(f"  {json.dumps(db, ensure_ascii=False)}: "
                     f"{json.dumps(name, ensure_ascii=False)}," for db in groups[name])
    if lines:
        lines[-1] = lines[-1].rstrip(",")
    text = "{\n" + "\n".join(lines) + "\n}\n"
    json.loads(text)  # 조립한 JSON 이 깨지지 않았는지 확인하고 쓴다
    qp.ensure_home()
    with open(qp.CONN_MAP_FILE, "w", encoding="utf-8") as fp:
        fp.write(text)


def update_conn_map(name, databases):
    """querypie-conn-map.json 에 database → 커넥션 이름 매핑을 병합한다."""
    mapping = {}
    if os.path.exists(qp.CONN_MAP_FILE):
        with open(qp.CONN_MAP_FILE, encoding="utf-8") as fp:
            mapping = json.load(fp)
    databases = [db for db in databases if db not in SYSTEM_DATABASES]
    added = [db for db in databases if mapping.get(db) != name]
    for db in databases:
        mapping[db] = name
    for db in SYSTEM_DATABASES:
        mapping.pop(db, None)
    dump_conn_map(mapping)
    return added


def short_name(cluster_name):
    """클러스터 이름 → 도구에 쓰는 커넥션 이름 (shop-cluster → shop)."""
    return cluster_name[:-len("-cluster")] if cluster_name.endswith("-cluster") else cluster_name


def cmd_list(args, window_id):
    clusters = list_clusters(args.insecure, window_id)
    if not clusters:
        print("  접근 가능한 클러스터가 없습니다.")
        return
    saved = {}
    for path in os.listdir(os.path.dirname(qp.OPEN_FILE_TMPL)):
        if not (path.startswith("querypie-open-") and path.endswith(".b64")):
            continue
        name = path[len("querypie-open-"):-len(".b64")]
        payload, _ = qp.load_open(name)
        if payload:
            raw = qp.extract_field_raw(payload, 2)
            if raw:
                saved[raw.decode("utf-8", "replace")] = name
    print(f"  {'커넥션 이름':14} {'종류':6} {'이름':70} 비고")
    for _, cluster_uuid, cluster_name in clusters:
        rows = list_instances(cluster_uuid, args.insecure, window_id)
        # 조회와 쓰기가 다른 엔드포인트로 갈 수 있어 둘 다 표시한다
        chosen = pick_instance(rows)
        chosen_writer = pick_instance(rows, prefer_writer=True)
        for conn_uuid, name, kind in rows:
            marks = []
            if chosen and conn_uuid == chosen[0]:
                marks.append("조회 기본")
            if chosen_writer and conn_uuid == chosen_writer[0]:
                marks.append("쓰기 기본")
            if conn_uuid in saved:
                marks.append(f"b64 {saved[conn_uuid]}")
            print(f"  {short_name(cluster_name):14} {kind:6} {name:70} {', '.join(marks) or '-'}")


def cmd_map(args, window_id):
    """클러스터의 database 목록을 받아 conn-map.json 을 갱신한다."""
    clusters = list_clusters(args.insecure, window_id)
    if args.name:
        low = args.name.lower()
        clusters = [c for c in clusters if low in c[2].lower()]
        if not clusters:
            sys.exit(f"'{args.name}' 에 맞는 클러스터가 없습니다. `list` 로 확인하세요.")
    for _, cluster_uuid, cluster_name in clusters:
        name = short_name(cluster_name)
        pick = pick_instance(list_instances(cluster_uuid, args.insecure, window_id))
        if not pick:
            print(f"  {name:14} 커넥션이 없어 건너뜁니다")
            continue
        # 세션은 창 단위라, 클러스터마다 새 창을 써야 앞 클러스터의 세션과 섞이지 않는다
        sub_window = secrets.token_hex(16)
        try:
            payload = open_payload_for(pick, args.insecure, sub_window)
            databases = fetch_databases(pick[0], payload, args.insecure, sub_window)
        except (ValueError, SystemExit) as e:
            # MySQL 이 아닌 커넥션(ClickHouse 등)은 여기서 갈린다 — 나머지는 계속 진행한다
            print(f"  {name:14} 갱신 실패: {str(e)[:90]}")
            continue
        if not databases:
            print(f"  {name:14} database 목록이 비어 건너뜁니다")
            continue
        added = update_conn_map(name, databases)
        print(f"  {name:14} database {len(databases)}개 중 {len(added)}개 추가/변경")


def cmd_add(args, window_id):
    """특정 커넥션을 b64 파일로 박제한다 (조립이 통하지 않을 때의 폴백)."""
    rows = collect(args.insecure, window_id)
    if args.match:
        rows = [r for r in rows if args.match.lower() in r[2].lower()]
    if not rows:
        sys.exit("조건에 맞는 커넥션이 없습니다. `list` 로 후보를 확인하세요.")
    if len(rows) > 1:
        print("후보가 여럿입니다. --match 로 좁혀 주세요:")
        for cluster_name, _, name, kind in rows:
            print(f"  {short_name(cluster_name):14} {kind:6} {name}")
        sys.exit(1)

    cluster_name, conn_uuid, host, kind = rows[0]
    out = qp.OPEN_FILE_TMPL.format(name=args.name)
    if os.path.exists(out) and not args.force:
        sys.exit(f"이미 있습니다: {os.path.basename(out)} (덮어쓰려면 --force)")

    payload = open_payload_for((conn_uuid, host, kind), args.insecure, window_id)

    qp.ensure_home()
    os.umask(0o077)
    with open(out, "w", encoding="utf-8") as fp:
        fp.write(base64.b64encode(payload).decode())
    print(f"저장 완료: {os.path.basename(out)} (payload={len(payload)}B)")
    print(f"  클러스터 {cluster_name} / {kind} {host}")
    print(f"  이제 '{args.name}' 은 조립 대신 이 파일을 씁니다. "
          f"노드가 교체되면 --force 로 다시 만들거나 파일을 지우세요.")


def main():
    ap = argparse.ArgumentParser(description="QueryPie 커넥션 목록과 database 매핑")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="클러스터와 커넥션 목록 (자동 선택 대상 표시)")
    p_map = sub.add_parser("map", help="database → 커넥션 이름 매핑 갱신")
    p_map.add_argument("name", nargs="?", help="갱신할 클러스터 (생략하면 전부)")
    p_add = sub.add_parser("add", help="[폴백] 특정 커넥션을 b64 파일로 박제")
    p_add.add_argument("name", help="커넥션 이름 (querypie-open-<name>.b64 로 저장된다)")
    p_add.add_argument("--match", help="호스트나 노드 이름 일부로 후보를 좁힌다")
    p_add.add_argument("--force", action="store_true", help="같은 이름이 있어도 덮어쓴다")

    for p in (p_list, p_map, p_add):
        p.add_argument("--insecure", action="store_true", help="TLS 검증 생략")
        p.add_argument("--login", action="store_true", help="실행 전 credential 로 강제 재로그인")
        p.add_argument("--window-id", default="", help="x-querypie-window-id (미지정 시 랜덤)")

    args = ap.parse_args()
    window_id = args.window_id or secrets.token_hex(16)
    if args.login and not qp.login(args.insecure, window_id):
        sys.exit(f"--login 실패. {qp.LOGIN_FILE} 를 확인하세요.")
    if not qp.has_cookies() and not qp.login(args.insecure, window_id):
        sys.exit("쿠키가 없고 자동 로그인도 하지 못했습니다.")
    qp._refresh_tokens(args.insecure, window_id) or qp.login(args.insecure, window_id)

    {"list": cmd_list, "map": cmd_map, "add": cmd_add}[args.cmd](args, window_id)


if __name__ == "__main__":
    main()
