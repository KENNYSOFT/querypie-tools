#!/usr/bin/env python3
"""QueryPie 쿼리 실행 승인 요청(SQL Execution Request) CLI.

웹 UI 의 "SQL 요청" 화면이 쓰는 gRPC-Web 인터페이스를 그대로 호출한다.
인증(쿠키/자동 로그인/토큰 갱신)과 커넥션 해석은 조회 도구 querypie_query.py 를
그대로 재사용하므로, 그 도구가 동작하는 환경이면 추가 준비가 필요 없다.

주의: submit 은 결재선에 지정한 사람들에게 실제 승인 요청을 보낸다(외부 영향).
      보내기 전에 `--dry-run` 으로 조립된 요청을 먼저 확인할 것.

사용 예:
  # 결재선에 넣을 수 있는 사람/그룹 목록
  python querypie_request.py assignees

  # 요청 목록(진행 중)
  python querypie_request.py list

  # 승인 요청 보내기 (결재자 3인 순차, 실행자 본인)
  python querypie_request.py submit \
      --db shop --sql-file schema/2026-08-1-after.sql \
      --title "2026.08.1 배포 후 쿼리" \
      --reason "https://github.com/<org>/<repo>/blob/main/schema/..." \
      --approver alice --approver bob --approver carol \
      --executor dave --dry-run

  # 한 단계에 여러 명(누구든 한 명 승인) 은 쉼표로
  python querypie_request.py submit ... --approver "alice,bob" --approver carol

결재자/실행자 지정:
  이름(alice) 또는 이메일(alice@example.com), 그룹명(DevOps, PM) 중 아무거나.
  대소문자 무시 · 정확히 일치하는 항목이 없으면 부분일치로 찾고, 후보가 여럿이면 중단한다.
  `--approver` 를 여러 번 주면 준 순서대로 결재 단계가 쌓인다.
  `--executor` 는 승인 후 그 쿼리를 실행할 사람으로, UI 와 동일하게 필수다 (보통 요청자 본인).
  요청자는 서버가 세션 사용자로 채우므로 따로 지정하지 않는다.

기간:
  --start 기본값은 오늘(KST), --end 기본값은 --start + 13일 (UI 기본값과 동일).
  --expire 는 미지정 시 --end 와 같은 값으로 보낸다.

DB/커넥션:
  --db 는 querypie-conn-map.json 매핑으로 커넥션을 자동 선택한다(조회 도구와 동일).
  매핑에 없으면 --conn-name 또는 --conn <uuid> 로 지정한다.
"""
import argparse
import os
import re
import secrets
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import querypie_query as qp  # noqa: E402  (경로 삽입 후 import)

KST = timezone(timedelta(hours=9))

SVC_EXECUTION = "api.workflow.sqlexecution.SqlExecutionRequestService"
SVC_EXPORT = "api.workflow.dataexport.DataExportRequestService"
SVC_RULE = "api.user.workflow.rule.UserWorkflowRuleService"
SVC_SENT = "api.workflow.SentRequestQueryService"

# Submit 요청 필드 번호. 필드 이름은 QueryPie 웹 번들의 protobuf 정의에서 확인했다
# (proto.api.workflow.sqlexecution.SubmitRequest / ...dataexport.SubmitRequest).
# 쿼리 실행 요청과 내보내기 요청은 희망 실행일 하나만 빼고 구조가 같다.
F_TITLE = 1          # 제목
F_OBJECT_TYPE = 2    # 대상 객체 종류 (DB 커넥션 = 3)
F_OBJECT_UUID = 3    # connectionUuid
F_DATABASE = 4       # databaseName
F_CONTENT = 6        # 실행/내보낼 SQL 전문
F_EXEC_UNTIL = 7     # 실행 만료일 (이 날짜까지 실행할 수 있다)
F_APPROVE_UNTIL = 8  # 승인 만료일 (이 날짜까지 승인되지 않으면 만료된다)
F_COMMENTS = 9       # 사유/참고 링크
F_APPROVAL = 10      # 결재선 {반복 #1 단계, #2 결재 방식}
F_EXECUTOR = 11      # 실행자 {반복 #1 대상} — 승인 후 이 쿼리를 실행할 사람.
                     # 요청자는 서버가 세션 사용자로 채우므로 이 요청에 담기지 않는다
                     # (상세 조회 응답에서 실행자 #13, 요청자 #18 로 따로 내려온다).
F_REVIEWER = 12      # 참조자 {반복 #1 대상}
F_URGENT = 13        # 긴급 여부 — 필드는 존재하지만 서버가 거부한다. 아래 --urgent 참고
F_RULE = 14          # 워크플로우 규칙 uuid
# 희망 실행일. 쿼리 실행 요청은 #20, 내보내기 요청은 #15 로 번호가 다르다.
F_PREFERRED_EXEC = 20
F_PREFERRED_EXPORT = 15

# 대상 객체 종류: DB 커넥션
OBJECT_TYPE_DATABASE = 3
# 한 단계에 여러 명일 때의 승인 조건 (proto.api.workflow.StepApproveCondition)
STEP_APPROVE_ALL = 0   # 그 단계의 전원이 승인해야 통과
STEP_APPROVE_ANY = 1   # 누구든 한 명이 승인하면 통과
# 이 도구의 기본값은 ANY 다. 근거는 UI 로 올린 요청을 캡처한 HAR 이 ANY 였다는 것뿐이라
# (UI 자체의 기본값은 확인하지 못했다), 단계에 여러 명을 넣을 때는 의도한 조건인지
# --approve-condition 으로 명시하는 편이 안전하다.

STATUS_LABEL = {1: "진행중", 2: "승인", 3: "반려", 4: "취소"}


# --------------------------------------------------------------------------
# 인증 준비 (조회 도구와 동일한 절차)
# --------------------------------------------------------------------------
def ensure_auth(insecure=False, window_id="", force_login=False, skip_refresh=False):
    """쿠키 확보 + 선제 토큰 갱신. querypie_query.main() 의 인증 준비와 같은 흐름."""
    if force_login and not qp.login(insecure, window_id):
        sys.exit(f"--login 실패. {qp.LOGIN_FILE} 를 확인하세요.")
    if not qp.has_cookies() and not qp.login(insecure, window_id):
        sys.exit("쿠키가 없고 자동 로그인도 하지 못했습니다. "
                 f'{qp.LOGIN_FILE} 에 {{"username": ..., "password": ...}} 를 두세요.')
    if skip_refresh:
        return
    if qp._refresh_tokens(insecure, window_id):
        qp.info("[refresh] 선제 토큰 갱신 완료")
    elif qp.login(insecure, window_id):
        pass  # refresh 가 죽었으면 credential 로 재로그인 (메시지는 login 이 남긴다)
    else:
        qp.info("[refresh] 갱신 실패 — 저장된 access token 의 남은 수명만 쓸 수 있습니다.")


# --------------------------------------------------------------------------
# 워크플로우 규칙 / 결재 대상
# --------------------------------------------------------------------------
def list_rules(insecure=False, window_id=""):
    """UserWorkflowRuleService/GetPage → [(uuid, name)] (요청 시 쓸 규칙 목록)."""
    payload = qp.f_vi(2, 2147483647) + qp.f_vi(5, 1)
    out = []
    for d in qp.call(SVC_RULE, "GetPage", payload, insecure, window_id=window_id):
        for fn, raw in qp.iter_fields(d):
            if fn != 2:
                continue
            items = qp.decode_raw(raw)
            uuid = qp.find_field(items, 1, "str")
            name = qp.find_field(items, 2, "str")
            if uuid:
                out.append((uuid, name or ""))
    return out


def resolve_rule(explicit, insecure=False, window_id=""):
    """규칙 uuid 결정. 미지정 시 규칙 목록의 첫 항목(계정에 적용되는 기본 규칙)."""
    if explicit:
        return explicit, ""
    rules = list_rules(insecure, window_id)
    if not rules:
        sys.exit("워크플로우 규칙을 찾지 못했습니다. --rule 로 규칙 uuid 를 직접 지정하세요.")
    return rules[0]


def get_assignees(rule_uuid, insecure=False, window_id=""):
    """GetAssigneeList → [{uuid, name, email, type, raw}].

    raw 는 응답 엔트리 원본 바이트로, Submit 의 결재선/참조자 메시지와 구조가 같아
    그대로 재사용한다(필드 재조립 없이 안전하게 전달).
    """
    payload = qp.f_str(1, rule_uuid)
    out = []
    for d in qp.call(SVC_RULE, "GetAssigneeList", payload, insecure, window_id=window_id):
        for fn, raw in qp.iter_fields(d):
            if fn != 1:
                continue
            items = qp.decode_raw(raw)
            out.append({
                "uuid": qp.find_field(items, 1, "str") or "",
                "type": qp.find_field(items, 2, "varint") or 0,
                "name": qp.find_field(items, 4, "str") or "",
                "email": qp.find_field(items, 5, "str") or "",
                "raw": raw,
            })
    return out


def match_assignee(token, assignees):
    """이름/이메일/이메일 local-part 로 결재 대상 1건을 찾는다 (없거나 여럿이면 중단)."""
    key = token.strip().lower()
    if not key:
        sys.exit("빈 결재 대상이 지정되었습니다.")

    def cands(pred):
        return [a for a in assignees if pred(a)]

    exact = cands(lambda a: key in (a["name"].lower(), a["email"].lower(),
                                    a["email"].split("@")[0].lower()))
    hits = exact or cands(lambda a: key in a["name"].lower() or key in a["email"].lower())
    if not hits:
        sys.exit(f"'{token}' 에 해당하는 결재 대상을 찾지 못했습니다. "
                 f"`assignees` 명령으로 목록을 확인하세요.")
    if len(hits) > 1:
        names = ", ".join(f'{a["name"]}({a["email"] or "그룹"})' for a in hits)
        sys.exit(f"'{token}' 이 여러 대상과 일치합니다: {names}")
    return hits[0]


def label(a):
    return f'{a["name"]} <{a["email"]}>' if a["email"] else f'{a["name"]} (그룹)'


# --------------------------------------------------------------------------
# Submit
# --------------------------------------------------------------------------
def build_approval(field, steps, condition):
    """결재선: 단계마다 field 를 하나씩 반복해 내보낸다.

    구조는 웹 번들의 protobuf 정의 기준이다.
      SubmitRequest.approvalSteps = repeated ApprovalStepSubmitRequest  (필드 #10)
      ApprovalStepSubmitRequest   = { #1: repeated AssigneeSubmitRequest, #2: 승인 조건 }
      AssigneeSubmitRequest       = { #1: Assignee }

    즉 단계는 #10 을 여러 번 내보내 표현하고, 한 단계의 여러 명은 그 안의 #1 을
    여러 번 내보내 표현한다. 각 대상은 AssigneeSubmitRequest 로 한 겹 감싼다 —
    감싸지 않고 #1 에 대상을 바로 넣으면 같은 필드가 덮어써져 마지막 사람만 남는다.

    steps 는 [[assignee, ...], ...] — 바깥 리스트가 결재 순서, 안쪽이 같은 단계의 대상.
    """
    out = b""
    for members in steps:
        step = b"".join(qp.f_msg(1, qp.f_msg(1, a["raw"])) for a in members)
        out += qp.f_msg(field, step + qp.f_vi(2, condition))
    return out


def build_assignees(field, people):
    """실행자/참조자: repeated AssigneeSubmitRequest 이므로 대상마다 field 를 반복한다."""
    return b"".join(qp.f_msg(field, qp.f_msg(1, a["raw"])) for a in people)


def submit(kind, rule_uuid, title, preferred, exec_until, approve_until, conn, database,
           sql, comments, steps, executors, reviewers=(), urgent=False,
           condition=STEP_APPROVE_ANY, insecure=False, window_id=""):
    """Submit → 생성된 요청 uuid. kind 는 'exec'(쿼리 실행) 또는 'export'(내보내기)."""
    export = kind == "export"
    payload = (
        qp.f_str(F_RULE, rule_uuid)
        + qp.f_str(F_TITLE, title)
        + qp.f_str(F_PREFERRED_EXPORT if export else F_PREFERRED_EXEC, preferred)
        + qp.f_str(F_EXEC_UNTIL, exec_until)
        + qp.f_vi(F_OBJECT_TYPE, OBJECT_TYPE_DATABASE)
        + qp.f_str(F_OBJECT_UUID, conn)
        + qp.f_str(F_DATABASE, database)
        + qp.f_str(F_CONTENT, sql)
        + qp.f_str(F_COMMENTS, comments)
        + qp.f_str(F_APPROVE_UNTIL, approve_until)
        + build_approval(F_APPROVAL, steps, condition)
        + build_assignees(F_EXECUTOR, executors)
        + build_assignees(F_REVIEWER, reviewers)
        + qp.f_vi(F_URGENT, 1 if urgent else 0)
    )
    frames = qp.call(SVC_EXPORT if export else SVC_EXECUTION, "Submit",
                     payload, insecure, window_id=window_id)
    for d in frames:
        uuid = qp.extract_field_raw(d, 1)
        if uuid:
            return uuid.decode("utf-8", "replace")
    return ""


# --------------------------------------------------------------------------
# 요청 목록
# --------------------------------------------------------------------------
def to_kst(iso):
    """'2026-08-06T02:11:09.790Z' → '2026-08-06 11:11 KST'."""
    m = re.match(r"(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})", iso or "")
    if not m:
        return iso or ""
    dt = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M KST")


def list_sent(method="GetInProgressPage", limit=50, insecure=False, window_id=""):
    """SentRequestQueryService → 내가 보낸 요청 목록.

    method 는 GetInProgressPage(진행 중) 또는 GetDonePage(완료)다. 완료 목록은 과거 요청의
    SQL 이나 실행 시간을 되짚을 때 쓴다. 목록에는 uuid 와 제목까지만 오므로 실행 시간 같은
    것은 uuid 로 상세 페이지를 열어 본다.
    """
    payload = qp.f_vi(9, limit)
    rows = []
    for d in qp.call(SVC_SENT, method, payload, insecure, window_id=window_id):
        for fn, raw in qp.iter_fields(d):
            if fn != 2:
                continue
            items = qp.decode_raw(raw)
            rows.append({
                "uuid": qp.find_field(items, 1, "str") or "",
                "status": qp.find_field(items, 2, "varint") or 0,
                "title": qp.find_field(items, 3, "str") or "",
                "created": qp.find_field(items, 6, "str") or "",
                "number": qp.find_field(items, 9, "varint") or 0,
            })
    return rows


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def add_common(ap):
    ap.add_argument("--rule", help="워크플로우 규칙 uuid (미지정 시 자동 선택)")
    ap.add_argument("--insecure", action="store_true", help="TLS 검증 생략")
    ap.add_argument("--window-id", default="", help="x-querypie-window-id (미지정 시 랜덤)")
    ap.add_argument("--login", action="store_true", help="실행 전 credential 로 강제 재로그인")
    ap.add_argument("--no-refresh", action="store_true", help="선제 토큰 갱신 생략")


def main():
    ap = argparse.ArgumentParser(description="QueryPie 쿼리 실행 승인 요청 CLI")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_sub = sub.add_parser("submit", help="승인 요청 보내기")
    add_common(p_sub)
    p_sub.add_argument("--title", required=True, help="요청 제목")
    p_sub.add_argument("--db", help="databaseName (커넥션 자동 선택에도 쓰인다)")
    p_sub.add_argument("--conn", help="connectionUuid (미지정 시 --db/--conn-name 으로 결정)")
    p_sub.add_argument("--conn-name", help="세션 페이로드 이름 (querypie-open-<name>.b64)")
    p_sub.add_argument("--sql", help="실행할 SQL")
    p_sub.add_argument("--sql-file", help="SQL 파일 경로")
    p_sub.add_argument("--reason", default="", help="사유 / 참고 링크")
    p_sub.add_argument("--approver", action="append", default=[],
                       help="결재자. 여러 번 주면 그 순서대로 결재 단계가 쌓이고, "
                            "쉼표로 묶으면 같은 단계에 들어간다")
    p_sub.add_argument("--executor", action="append", default=[],
                       help="승인 후 쿼리를 실행할 사람 (쉼표 구분으로 여러 명). 필수")
    p_sub.add_argument("--kind", choices=("exec", "export"), default="exec",
                       help="exec = 쿼리 실행 요청(기본), export = SQL 결과 내보내기 요청")
    p_sub.add_argument("--reviewer", action="append", default=[],
                       help="참조자 (쉼표 구분으로 여러 명)")
    # 서버가 거부하므로 쓰지 말 것 (2026-08-12 실제 제출로 확인). protobuf 정의에는
    # 있어서 조립은 되지만 제출이 실패한다. 나중에 서버 정책이 바뀔 때를 위해 남겨 둔다.
    p_sub.add_argument("--urgent", action="store_true",
                       help="[사용 불가] 긴급 표시. 서버가 거부해 제출이 실패한다")
    p_sub.add_argument("--approve-condition", choices=("any", "all"), default="any",
                       help="한 단계에 여러 명일 때 any = 한 명만 승인하면 통과(기본), "
                            "all = 전원 승인 필요")
    p_sub.add_argument("--preferred-date", help="희망 실행일 YYYY-MM-DD (기본: 오늘 KST)")
    p_sub.add_argument("--exec-until", help="실행 만료일 YYYY-MM-DD — 이 날짜까지 실행할 수 있다 "
                                            "(기본: 희망 실행일 +13일)")
    p_sub.add_argument("--approve-until", help="승인 만료일 YYYY-MM-DD — 이 날짜까지 승인되지 "
                                               "않으면 만료된다 (기본: 실행 만료일과 동일)")
    p_sub.add_argument("--dry-run", action="store_true",
                       help="보내지 않고 조립된 요청 내용만 출력")

    p_list = sub.add_parser("list", help="내가 보낸 진행 중 요청 목록")
    add_common(p_list)
    p_list.add_argument("--limit", type=int, default=50)

    p_done = sub.add_parser("done", help="내가 보낸 완료된 요청 목록 (과거 요청의 SQL·실행 시간 되짚기)")
    add_common(p_done)
    p_done.add_argument("--limit", type=int, default=50)
    p_done.add_argument("--grep", default="", help="제목에 이 문자열이 든 것만 (대소문자 무시)")

    p_as = sub.add_parser("assignees", help="결재선에 넣을 수 있는 사람/그룹 목록")
    add_common(p_as)

    p_rules = sub.add_parser("rules", help="워크플로우 규칙 목록")
    add_common(p_rules)

    args = ap.parse_args()
    window_id = args.window_id or secrets.token_hex(16)
    ensure_auth(args.insecure, window_id, args.login, args.no_refresh)

    if args.cmd == "rules":
        for uuid, name in list_rules(args.insecure, window_id):
            print(f"  {name}  [{uuid}]")
        return

    if args.cmd in ("list", "done"):
        done = args.cmd == "done"
        rows = list_sent("GetDonePage" if done else "GetInProgressPage",
                         args.limit, args.insecure, window_id)
        needle = getattr(args, "grep", "").lower()
        if needle:
            rows = [r for r in rows if needle in r["title"].lower()]
        if not rows:
            print("  (완료된 요청 없음)" if done else "  (진행 중인 요청 없음)")
            return
        for r in rows:
            # 완료 목록의 필드 2 는 진행 중 목록의 상태와 값이 달라(완료 건에도 1 이 온다)
            # STATUS_LABEL 로 읽으면 "진행중" 으로 잘못 찍힌다. 의미를 확인하기 전까지 표시하지 않는다.
            head = "" if done else f'[{STATUS_LABEL.get(r["status"], str(r["status"]))}] '
            print(f'  #{r["number"]} {head}{to_kst(r["created"])}  {r["title"]}')
            print(f'      {r["uuid"]}')
        return

    rule_uuid, rule_name = resolve_rule(args.rule, args.insecure, window_id)

    if args.cmd == "assignees":
        print(f"  규칙: {rule_name or rule_uuid}")
        for a in get_assignees(rule_uuid, args.insecure, window_id):
            print(f"  - {label(a)}")
        return

    # ---- submit ----
    sql = args.sql
    if args.sql_file:
        with open(args.sql_file, encoding="utf-8") as fp:
            sql = fp.read()
    if not sql:
        ap.error("--sql 또는 --sql-file 이 필요합니다")
    if not args.approver:
        ap.error("--approver 가 최소 1명 필요합니다 (`assignees` 명령으로 목록 확인)")
    if not args.executor:
        ap.error("--executor 가 필요합니다 — UI 와 동일하게 실행자는 필수입니다 "
                 "(승인 후 쿼리를 실행할 사람, 보통 요청자 본인)")

    db = args.db or ""
    conn = args.conn
    conn_src = "--conn 으로 직접 지정"
    if not conn:
        conn_name = qp.resolve_conn_name(db, args.conn_name)
        # 요청에 실리는 SQL 은 승인 후 실제로 실행되므로 읽기 엔드포인트로 보내면 안 된다
        open_payload, src = qp.resolve_open_payload(conn_name, args.insecure, window_id,
                                                    prefer_writer=True)
        if not open_payload:
            ap.error(f"'{db}' 에 쓸 커넥션을 찾지 못했습니다. --conn 으로 uuid 를 직접 주거나 "
                     f"querypie-conn-map.json 에 매핑을 추가하세요")
        raw = qp.extract_field_raw(open_payload, 2)
        if not raw:
            ap.error(f"{src} 에서 connectionUuid 를 찾지 못했습니다")
        conn = raw.decode("utf-8", "replace")
        conn_src = src
    if not db:
        ap.error("--db (databaseName) 가 필요합니다")

    preferred = args.preferred_date or datetime.now(KST).strftime("%Y-%m-%d")
    if args.exec_until:
        exec_until = args.exec_until
    else:
        exec_until = (datetime.strptime(preferred, "%Y-%m-%d")
                      + timedelta(days=13)).strftime("%Y-%m-%d")
    approve_until = args.approve_until or exec_until

    assignees = get_assignees(rule_uuid, args.insecure, window_id)
    steps = [[match_assignee(t, assignees) for t in step.split(",") if t.strip()]
             for step in args.approver]
    executors = [match_assignee(t, assignees)
                 for group in args.executor for t in group.split(",") if t.strip()]
    if not executors:
        ap.error("--executor 에서 실행자를 하나도 해석하지 못했습니다 (빈 값인지 확인)")
    reviewers = [match_assignee(t, assignees)
                 for group in args.reviewer for t in group.split(",") if t.strip()]

    print(f"  요청 종류 : {'SQL 내보내기' if args.kind == 'export' else '쿼리 실행'}"
          + ("  [긴급]" if args.urgent else ""))
    print(f"  규칙     : {rule_name or rule_uuid}")
    print(f"  제목     : {args.title}")
    print(f"  대상 DB  : {db}")
    print(f"  커넥션   : {conn_src}")
    print(f"  희망 실행일: {preferred}  (실행 만료 {exec_until} / 승인 만료 {approve_until})")
    print(f"  사유     : {args.reason or '(없음)'}")
    for i, members in enumerate(steps, 1):
        cond = ""
        if len(members) > 1:
            cond = "  (전원 승인)" if args.approve_condition == "all" else "  (한 명만 승인)"
        print(f"  결재 {i}단계 : " + ", ".join(label(a) for a in members) + cond)
    print(f"  실행자   : " + (", ".join(label(a) for a in executors) or "(없음)"))
    print(f"  참조자   : " + (", ".join(label(a) for a in reviewers) or "(없음)"))
    print(f"  SQL      : {len(sql)}자")
    print("  " + "-" * 60)
    for line in sql.splitlines():
        print("  | " + line)
    print("  " + "-" * 60)

    if args.dry_run:
        print("  [dry-run] 보내지 않았습니다.")
        return

    condition = STEP_APPROVE_ALL if args.approve_condition == "all" else STEP_APPROVE_ANY
    uuid = submit(args.kind, rule_uuid, args.title, preferred, exec_until, approve_until,
                  conn, db, sql, args.reason, steps, executors, reviewers, args.urgent,
                  condition, args.insecure, window_id)
    print(f"  요청 완료: {uuid}" if uuid else "  요청은 보냈으나 응답에서 uuid 를 찾지 못했습니다.")


if __name__ == "__main__":
    main()
