#!/usr/bin/env python3
"""QueryPie 쿼리 실행 승인 요청(SQL Execution Request) CLI.

웹 UI 의 "SQL 요청" 화면이 쓰는 gRPC-Web 인터페이스를 그대로 호출한다.
인증(쿠키/자동 로그인/토큰 갱신)과 커넥션 해석은 조회 도구 querypie_query.py 를
그대로 재사용하므로, 그 도구가 동작하는 환경이면 추가 준비가 필요 없다.

주의: submit 은 결재선에 지정한 사람들에게 실제 승인 요청을 보낸다(외부 영향).
      보내기 전에 `--dry-run` 으로 조립된 요청을 먼저 확인할 것.
      approve 는 받은 요청을 실제로 승인한다(외부 영향). 승인하면 그 단계의 결재가 끝나
      요청자가 곧바로 실행할 수 있고 되돌리는 수단이 없다. 먼저 `--dry-run` 으로 대상과
      내 결재 차례인지, 사유의 GitHub 링크와 SQL 을 비교한 결과를 확인할 것.
      inbox 와 show 는 조회만 한다 (승인이나 반려는 하지 않는다).

사용 예:
  # 결재선에 넣을 수 있는 사람/그룹 목록
  python querypie_request.py assignees

  # 요청 목록(진행 중)
  python querypie_request.py list

  # 나에게 온 승인 요청 (기본은 결재 대기, --done 이면 결재가 끝난 것)
  python querypie_request.py inbox
  python querypie_request.py inbox --done --limit 20

  # 요청 한 건의 SQL, 사유, 결재 의견 (보낸 요청과 받은 요청 모두)
  python querypie_request.py show <uuid>

  # 받은 요청 승인 (먼저 --dry-run 으로 대상과 링크 비교 결과를 본다)
  python querypie_request.py approve <uuid> [<uuid> ...] --dry-run
  python querypie_request.py approve <uuid> [<uuid> ...] --comment "승인합니다."

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

승인(approve):
  요청마다 지금 내 결재 차례인지(결재 대기 목록에 있는지) 보고, 아니면 보내지 않고 건너뛴다.
  사유에 GitHub 파일 링크가 있으면 그 파일과 요청 SQL 을 비교해 찍는다. 비공개 저장소를
  읽어야 해 gh CLI 를 쓰고, gh 가 없으면 비교만 건너뛴다.
  승인 뒤에는 상세를 다시 열어 이번 호출로 승인으로 바뀐 결재자와 그 의견을 확인한다.
  의견을 주지 않으면 웹 UI 의 기본 의견과 같은 '승인합니다.' 가 들어간다.
"""
import argparse
import difflib
import os
import re
import secrets
import shutil
import subprocess
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import querypie_query as qp  # noqa: E402  (경로 삽입 후 import)

KST = timezone(timedelta(hours=9))

SVC_EXECUTION = "api.workflow.sqlexecution.SqlExecutionRequestService"
SVC_EXPORT = "api.workflow.dataexport.DataExportRequestService"
SVC_RULE = "api.user.workflow.rule.UserWorkflowRuleService"
SVC_SENT = "api.workflow.SentRequestQueryService"
SVC_RECEIVED = "api.workflow.ReceivedRequestQueryService"
SVC_WORKFLOW = "api.workflow.WorkflowRequestQueryService"
SVC_ACTION = "api.workflow.WorkflowRequestActionService"

# 웹 UI 의 한국어 기본 승인 의견 (page.workflow.receivedRequest.todo.detail.msg.approval.defaultForApprove)
DEFAULT_APPROVE_COMMENT = "승인합니다."

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

# 요청의 승인 상태 (proto.api.workflow.ApprovalStatus). 0 은 proto3 기본값이라 응답에서 빠져 온다.
APPROVAL_STATUS = {0: "대기", 1: "진행중", 2: "승인", 3: "반려", 4: "취소", 5: "만료", 6: "임시저장"}
# 요청 종류 (proto.api.workflow.RequestType). 목록 항목의 #2 가 이 값이다.
REQUEST_TYPE = {0: "DB 접근", 1: "쿼리 실행", 2: "내보내기", 3: "서버 접근", 4: "접근 역할",
                5: "마스킹 해제", 6: "서버 권한", 7: "웹앱 접근", 8: "복호화",
                9: "제한 데이터 접근", 10: "DB 정책 예외", 11: "IP 등록", 12: "스키마 접근"}
# 상세를 풀 수 있는 요청 종류와 그 상세 조회 서비스
DETAIL_KIND = {1: "exec", 2: "export"}

# 상세 조회 응답의 상태 이름. 모두 0 이 proto3 기본값이라 응답에서 빠져 온다.
STEP_STATUS = {0: "-", 2: "진행중", 3: "승인", 4: "반려"}          # ApprovalStepStatus
APPROVER_STATUS = {0: "-", 1: "대기", 2: "승인", 3: "반려"}        # ApprovalAssigneeStatus
APPROVER_APPROVED = 2
EXECUTOR_STATUS = {0: "-", 1: "대기", 2: "실행함", 3: "취소"}      # ExecutionAssigneeStatus
REVIEWER_STATUS = {0: "-", 1: "안 읽음", 2: "확인"}                # ReviewAssigneeStatus
EXECUTION_STATUS = {0: "-", 1: "대기", 2: "실행 중", 3: "성공", 4: "실패", 5: "취소", 6: "만료"}
# 내용(#7)의 종류 (#6). 쿼리 실행은 sqlexecution.ContentType, 내보내기는 dataexport.ContentType.
CONTENT_TYPE = {"exec": {0: "SQL", 1: "파일"}, "export": {0: "쿼리 결과", 1: "테이블"}}

# GetDetailResponse 필드 번호 (sqlexecution / dataexport). 두 종류는 뼈대가 같고 아래 셋만 다르다.
# 공통: #2 제목, #5 커넥션{#2 클러스터 그룹, #10 database}, #6 내용 종류, #7 내용(SQL 전문),
#       #8 실행 만료일, #9 승인 만료일, #10 사유, #11 결재 단계(반복), #12 승인 상태,
#       #13 실행자(반복), #14 실행 상태, #15 참조자(반복), #16 긴급, #17 요청 시각, #18 요청자.
# 결재 단계 = {#2 결재자(반복), #3 승인 조건, #5 단계 상태}
# 결재자 = {#1 사람, #2 상태, #3 처리한 사람, #4 처리 시각, #5 결재 의견, #6 위임받은 사람}
# 실행자 = {#1 사람, #2 상태, #3 처리한 사람, #4 처리 시각, #5 끝난 시각, #6 의견}
# 참조자 = {#1 사람, #2 상태, #3 처리한 사람, #4 처리 시각, #5 의견}
DETAIL_FIELDS = {
    "exec": {"rule": 20, "preferred": 27, "canceled": 21},
    "export": {"rule": 19, "preferred": 21, "canceled": 20},
}


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

    항목은 GetSentRequestPageResponse.Response 로, #2 가 요청 종류이고 승인 상태는 #4 다.
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
                "type": qp.find_field(items, 2, "varint") or 0,
                "status": qp.find_field(items, 4, "varint") or 0,
                "title": qp.find_field(items, 3, "str") or "",
                "created": qp.find_field(items, 6, "str") or "",
                "number": qp.find_field(items, 9, "varint") or 0,
            })
    return rows


# --------------------------------------------------------------------------
# 받은 요청 / 요청 상세
# --------------------------------------------------------------------------
def pb(b):
    """protobuf 메시지를 {필드 번호: [값, ...]} 로 푼다. varint 는 int, 길이 구분 필드는 bytes.

    querypie_query.decode_raw 는 스키마 없이 추정하므로, UTF-8 로 읽히는 중첩 메시지
    (결재자나 요청자처럼)를 문자열로 잡는다. 정의를 아는 자리에서는 이것으로 풀고
    각 필드를 문자열로 읽을지 메시지로 읽을지는 호출부가 정한다. 형식이 깨졌으면 멈춘다.
    """
    out, i = {}, 0
    try:
        while i < len(b):
            key, i = qp.read_vi(b, i)
            fn, wt = key >> 3, key & 7
            if fn == 0:
                raise ValueError("필드 번호 0")
            if wt == 0:
                v, i = qp.read_vi(b, i)
            elif wt == 2:
                ln, i = qp.read_vi(b, i)
                v, i = b[i:i + ln], i + ln
            elif wt in (1, 5):
                size = 8 if wt == 1 else 4
                v, i = b[i:i + size], i + size
            else:
                raise ValueError(f"#{fn} 의 wire type {wt}")
            if i > len(b):
                raise ValueError(f"#{fn} 이 잘렸습니다")
            out.setdefault(fn, []).append(v)
    except (IndexError, ValueError) as e:
        sys.exit(f"응답을 해석하지 못했습니다 ({e}). QueryPie 의 응답 형식이 바뀌었을 수 있습니다.")
    return out


def pb_str(msg, fn):
    v = msg.get(fn)
    return v[0].decode("utf-8", "replace") if v else ""


def pb_int(msg, fn):
    v = msg.get(fn)
    return v[0] if v else 0


def pb_msg(msg, fn):
    v = msg.get(fn)
    return pb(v[0]) if v else {}


def pb_msgs(msg, fn):
    return [pb(v) for v in msg.get(fn, [])]


def who(user):
    """사람 메시지 → '이름 <이메일>'. api.workflow.User 와 common.UserResponse 모두 #3 이름, #4 이메일.

    오래된 요청(2023년)의 실행자 칸에는 같은 사람의 두 값이 뒤바뀌어 저장된 것이 있어,
    이름 칸에만 @ 가 있으면 바꿔 읽는다.
    """
    name, email = pb_str(user, 3), pb_str(user, 4)
    if "@" in name and "@" not in email:
        name, email = email, name
    return f"{name} <{email}>" if email else name


def list_received(method="GetToDoPage", limit=50, page=0, insecure=False, window_id=""):
    """ReceivedRequestQueryService → (나에게 결재가 걸린 요청 목록, 전체 건수).

    method 는 GetToDoPage(지금 내가 결재할 차례인 것) 또는 GetDonePage(결재가 끝난 것)다.
    요청 메시지(GetReceivedRequestPageRequest)는 보낸 요청 쪽과 달리 실행 상태 필드가 없어
    페이지 번호가 #7, 페이지 크기가 #8 로 한 칸씩 당겨져 있다. #9 는 검색어 문자열이라, 보낸
    요청처럼 #9 에 크기를 넣으면 서버가 status=10 으로 거절한다. 페이지는 0 부터 센다.

    항목(GetReceivedRequestPageResponse.Response)은 #1 uuid, #2 요청 종류, #3 제목, #4 요청자,
    #6 승인 상태, #7 요청 시각, #9 긴급, #10 목록 번호이고, 응답 #1(페이지 정보)의 #4 가 전체 건수다.
    """
    payload = qp.f_vi(7, page) + qp.f_vi(8, limit)
    rows, total = [], 0
    for d in qp.call(SVC_RECEIVED, method, payload, insecure, window_id=window_id):
        top = pb(d)
        total = pb_int(pb_msg(top, 1), 4)
        for item in pb_msgs(top, 2):
            rows.append({
                "uuid": pb_str(item, 1),
                "type": pb_int(item, 2),
                "title": pb_str(item, 3),
                "requester": pb_str(pb_msg(item, 4), 3),
                "status": pb_int(item, 6),
                "created": pb_str(item, 7),
                "urgent": bool(pb_int(item, 9)),
                "number": pb_int(item, 10),
            })
    return rows, total


def request_type(uuid, insecure=False, window_id=""):
    """WorkflowRequestQueryService/GetRequestType → 요청 종류 (REQUEST_TYPE 의 키).

    없는 uuid 는 서버가 'Not found workflow request' 로 거절한다.
    """
    frames = qp.call(SVC_WORKFLOW, "GetRequestType", qp.f_str(1, uuid), insecure,
                     window_id=window_id)
    if not frames:
        sys.exit(f"요청 종류 응답이 비어 있습니다 ({uuid}).")
    return pb_int(pb(frames[0]), 1)


def get_detail(uuid, kind, insecure=False, window_id=""):
    """GetDetail → 풀린 응답. kind 는 'exec'(쿼리 실행) 또는 'export'(내보내기)다.

    종류를 잘못 고르면 서버가 'Not found workflow request' 로 거절하므로 request_type 으로
    먼저 가린다. GetDetailRequest 의 #2 는 누구 입장에서 여는지(요청자, 결재자, 실행자, 참조자)인데,
    받은 요청을 요청자(0)와 결재자(1)로 열어 보니 응답이 바이트까지 같아 보내지 않는다.
    uuid 만 있으면 보낸 사람과 무관하게 열린다.
    """
    svc = SVC_EXPORT if kind == "export" else SVC_EXECUTION
    frames = qp.call(svc, "GetDetail", qp.f_str(1, uuid), insecure, window_id=window_id)
    if not frames:
        sys.exit(f"상세 응답이 비어 있습니다 ({uuid}).")
    return pb(frames[0])


def print_text(head, text, empty="(없음)", indent=13):
    """head 뒤에 text 를 붙인다. 여러 줄이면 둘째 줄부터 indent 칸 들여 쓴다."""
    lines = (text or "").splitlines() or [empty]
    print(head + lines[0])
    for line in lines[1:]:
        print(" " * indent + line)


def person_line(p, status_names, times):
    """결재자/실행자/참조자 한 사람 → '- 이름 <이메일>  [상태]  처리 시각'.

    times 는 [(필드 번호, 이름표)] 다. 처리한 사람(#3)이 본인과 다르면 그 이름을 붙인다.
    """
    user = who(pb_msg(p, 1))
    status = pb_int(p, 2)
    line = f"    - {user}  [{status_names.get(status, str(status))}]"
    for fn, name in times:
        at = pb_str(p, fn)
        if at:
            line += f"  {name} {to_kst(at)}"
    actor = who(pb_msg(p, 3))
    if actor and actor != user:
        line += f"  (처리: {actor})"
    return line


def print_detail(uuid, kind, d):
    """상세 응답을 리뷰하기 좋은 순서로 찍는다: 요약, 사유, 결재선과 의견, 실행, 참조, SQL."""
    f = DETAIL_FIELDS[kind]
    status = pb_int(d, 12)
    urgent = "  [긴급]" if pb_int(d, 16) else ""
    print(f"  [{APPROVAL_STATUS.get(status, str(status))}] "
          f"{'쿼리 실행' if kind == 'exec' else '내보내기'} 요청{urgent}")
    print(f"  uuid     : {uuid}")
    print(f"  제목     : {pb_str(d, 2)}")
    print(f"  요청자   : {who(pb_msg(d, 18))}  ({to_kst(pb_str(d, 17))})")
    print(f"  규칙     : {pb_str(d, f['rule'])}")
    conn = pb_msg(d, 5)
    cluster = pb_str(conn, 2)
    print(f"  대상 DB  : {pb_str(conn, 10)}" + (f"  ({cluster})" if cluster else ""))
    print(f"  희망 실행일: {pb_str(d, f['preferred'])}  "
          f"(실행 만료 {pb_str(d, 8)} / 승인 만료 {pb_str(d, 9)})")
    canceled = pb_str(d, f["canceled"])
    if canceled:
        print(f"  취소     : {to_kst(canceled)}")
    print_text("  사유     : ", pb_str(d, 10))

    steps = pb_msgs(d, 11)
    if not steps:
        print("  결재     : (결재선 없음)")
    for i, step in enumerate(steps, 1):
        people = pb_msgs(step, 2)
        cond = ""
        if len(people) > 1:
            cond = "  (전원 승인)" if pb_int(step, 3) == STEP_APPROVE_ALL else "  (한 명만 승인)"
        st = pb_int(step, 5)
        print(f"  결재 {i}단계 : {STEP_STATUS.get(st, str(st))}{cond}")
        for p in people:
            line = person_line(p, APPROVER_STATUS, [(4, "처리")])
            delegated = who(pb_msg(p, 6))
            if delegated:
                line += f"  (위임: {delegated})"
            print(line)
            if pb_str(p, 5):
                print_text("      의견 : ", pb_str(p, 5))

    ex = pb_int(d, 14)
    print(f"  실행     : {EXECUTION_STATUS.get(ex, str(ex))}")
    for p in pb_msgs(d, 13):
        print(person_line(p, EXECUTOR_STATUS, [(4, "처리"), (5, "종료")]))
        if pb_str(p, 6):
            print_text("      의견 : ", pb_str(p, 6))

    reviewers = pb_msgs(d, 15)
    print("  참조자   :" + ("" if reviewers else " (없음)"))
    for p in reviewers:
        print(person_line(p, REVIEWER_STATUS, [(4, "처리")]))
        if pb_str(p, 5):
            print_text("      의견 : ", pb_str(p, 5))

    ctype = pb_int(d, 6)
    content = pb_str(d, 7)
    print(f"  내용     : {CONTENT_TYPE[kind].get(ctype, str(ctype))}, {len(content)}자")
    print("  " + "-" * 60)
    for line in content.splitlines():
        print("  | " + line)
    print("  " + "-" * 60)


# --------------------------------------------------------------------------
# 받은 요청 승인
# --------------------------------------------------------------------------
# 사유에 적힌 GitHub 파일 링크. ref 에 / 가 든 브랜치는 첫 조각만 ref 로 읽혀 파일을 받지 못한다.
GITHUB_BLOB_RE = re.compile(r"https://github\.com/([\w.-]+)/([\w.-]+)/blob/([^/\s]+)/([^\s#?)>\]]+)")
# 링크와 SQL 이 다를 때 찍을 차이 줄 수
DIFF_LINES = 20


def fetch_github_file(owner, repo, ref, path):
    """저장소 파일 원문을 gh CLI 로 받는다. (bytes, None) 또는 (None, 받지 못한 사유).

    비공개 저장소도 읽어야 해 gh 의 인증을 빌린다. gh 가 없으면 비교만 건너뛴다.
    """
    gh = shutil.which("gh")
    if not gh:
        return None, "gh 가 없어 비교하지 않았습니다"
    api = (f"repos/{owner}/{repo}/contents/{urllib.parse.quote(path)}"
           f"?ref={urllib.parse.quote(ref, safe='')}")
    try:
        r = subprocess.run([gh, "api", "-H", "Accept: application/vnd.github.raw", api],
                           capture_output=True, timeout=60)
    except subprocess.TimeoutExpired:
        return None, "파일을 받지 못했습니다 (60초 안에 응답이 없음)"
    if r.returncode != 0:
        err = r.stderr.decode("utf-8", "replace").strip().splitlines()
        why = err[-1] if err else f"gh 종료 코드 {r.returncode}"
        return None, f"파일을 받지 못했습니다 ({why})"
    return r.stdout, None


def normalize_eol(text):
    """줄 끝을 LF 로 맞추고 파일 끝 개행을 걷어낸다. 붙여 넣으며 생기는 차이만 지운다."""
    return text.replace("\r\n", "\n").replace("\r", "\n").rstrip("\n")


def compare_links(reason, content):
    """사유의 GitHub 파일 링크마다 요청 SQL 과 비교한다. [(링크, 판정, 차이 줄)].

    판정은 '같음', '같음 (줄 끝이나 파일 끝 개행만 다름)', '다름', 또는 받지 못한 사유다.
    """
    out = []
    for m in GITHUB_BLOB_RE.finditer(reason or ""):
        owner, repo, ref, raw_path = m.groups()
        path = urllib.parse.unquote(raw_path.rstrip(".,;:"))
        data, err = fetch_github_file(owner, repo, ref, path)
        if data is None:
            out.append((m.group(0), err, []))
            continue
        text = data.decode("utf-8-sig", "replace")
        if text == content:
            out.append((m.group(0), "같음", []))
        elif normalize_eol(text) == normalize_eol(content):
            out.append((m.group(0), "같음 (줄 끝이나 파일 끝 개행만 다름)", []))
        else:
            diff = list(difflib.unified_diff(normalize_eol(text).split("\n"),
                                             normalize_eol(content).split("\n"),
                                             "링크 파일", "요청 SQL", lineterm="", n=1))
            more = [f"... ({len(diff) - DIFF_LINES}줄 더)"] if len(diff) > DIFF_LINES else []
            out.append((m.group(0), "다름", diff[:DIFF_LINES] + more))
    return out


def link_verdict(results):
    """요청 한 건의 링크 비교 결과를 집계용 낱말 하나로 줄인다."""
    if not results:
        return "링크 없음"
    verdicts = [v for _, v, _ in results]
    if "다름" in verdicts:
        return "다름"
    if all(v.startswith("같음") for v in verdicts):
        return "같음"
    return "비교 못 함"


def todo_requests(insecure=False, window_id=""):
    """지금 내 결재 차례인 요청을 {uuid: 목록 항목} 으로 모은다. 쪽을 넘기며 전부 받는다."""
    rows = {}
    for page in range(100):
        batch, total = list_received("GetToDoPage", 50, page, insecure, window_id)
        rows.update((r["uuid"], r) for r in batch)
        if not batch or len(rows) >= total:
            break
    return rows


def approver_states(d):
    """상세 응답의 결재자마다 (단계 번호, 사람) 을 (상태, 의견) 에 대응시킨다."""
    out = {}
    for i, step in enumerate(pb_msgs(d, 11), 1):
        for p in pb_msgs(step, 2):
            out[(i, who(pb_msg(p, 1)))] = (pb_int(p, 2), pb_str(p, 5))
    return out


def approve(uuid, comments, insecure=False, window_id=""):
    """WorkflowRequestActionService/Approve 로 요청 한 건을 승인한다 (외부 영향).

    ApproveRequest 는 #1 uuid, #2 의견, #3 accessControl 이다. 웹 UI 는 쿼리 실행과 내보내기
    요청에 #1, #2 만 보내고, #3(승인할 클러스터 목록)은 DB 접근 제어 요청에서만 채운다.
    응답은 빈 메시지라 결과는 상세를 다시 열어 확인한다.
    """
    qp.call(SVC_ACTION, "Approve", qp.f_str(1, uuid) + qp.f_str(2, comments), insecure,
            window_id=window_id)


def approve_requests(uuids, comments, dry_run=False, insecure=False, window_id=""):
    """요청마다 내 결재 차례인지와 링크 비교 결과를 찍고, dry_run 이 아니면 승인한 뒤 확인한다.

    내 차례가 아니거나 쿼리 실행, 내보내기 요청이 아니면 보내지 않고 건너뛴다. 로그인한
    사람을 알려 주는 API 가 없어, 승인 뒤에는 이번 호출로 '대기' 에서 '승인' 으로 바뀐
    결재자로 나를 찾는다. 그런 사람이 정확히 한 명이고 의견이 보낸 문구와 같아야 하며,
    아니면 그 자리에서 멈춘다. 승인 호출 자체가 실패하면 qp.call 이 멈춘다.
    """
    todo = todo_requests(insecure, window_id)
    approved, skipped, verdicts = 0, 0, {}
    for uuid in dict.fromkeys(uuids):
        row = todo.get(uuid)
        rtype = request_type(uuid, insecure, window_id)
        kind = DETAIL_KIND.get(rtype)
        print()
        if not kind:
            print(f"  [건너뜀] {uuid}: '{REQUEST_TYPE.get(rtype, rtype)}' 요청은 승인하지 않습니다 "
                  "(쿼리 실행과 내보내기만)")
            skipped += 1
            continue
        d = get_detail(uuid, kind, insecure, window_id)
        content = pb_str(d, 7)
        conn = pb_msg(d, 5)
        cluster = pb_str(conn, 2)
        number = f"#{row['number']} " if row else ""
        print(f"  {number}{pb_str(d, 2)}")
        print(f"      uuid    : {uuid}")
        print(f"      요청자  : {who(pb_msg(d, 18))}  ({to_kst(pb_str(d, 17))})")
        print(f"      대상 DB : {pb_str(conn, 10)}" + (f"  ({cluster})" if cluster else ""))
        print(f"      SQL     : {len(content)}자")
        results = compare_links(pb_str(d, 10), content)
        if not results:
            print("      링크    : (사유에 GitHub 파일 링크가 없음)")
        for url, verdict, diff in results:
            print(f"      링크    : {url}")
            print(f"                {verdict}")
            for line in diff:
                print(f"                | {line}")
        if not row:
            status = pb_int(d, 12)
            print(f"      결재    : [건너뜀] 내 결재 대기 목록에 없습니다 "
                  f"(요청 상태 {APPROVAL_STATUS.get(status, str(status))})")
            skipped += 1
            continue
        verdict = link_verdict(results)
        verdicts[verdict] = verdicts.get(verdict, 0) + 1
        if dry_run:
            print("      결재    : 내 결재 차례 (승인 대상)")
            continue
        before = approver_states(d)
        approve(uuid, comments, insecure, window_id)
        after = approver_states(get_detail(uuid, kind, insecure, window_id))
        changed = [k for k, (st, _) in after.items()
                   if st == APPROVER_APPROVED and before.get(k, (0, ""))[0] != APPROVER_APPROVED]
        if len(changed) != 1 or after[changed[0]][1].strip() != comments.strip():
            sys.exit(f"      [확인 실패] 승인은 보냈지만 이번에 승인으로 바뀐 결재자를 한 명으로 "
                     f"확인하지 못했습니다 ({len(changed)}명). `show {uuid}` 로 확인하세요.")
        print(f"      결재    : 승인함  {changed[0][1]}  의견: {after[changed[0]][1]}")
        approved += 1

    print()
    counts = ", ".join(f"{k} {v}건" for k, v in verdicts.items()) or "-"
    if dry_run:
        print(f"  [dry-run] 보내지 않았습니다. 승인 대상 {sum(verdicts.values())}건 "
              f"(링크 비교: {counts}), 건너뜀 {skipped}건")
    else:
        print(f"  승인 {approved}건 (링크 비교: {counts}), 건너뜀 {skipped}건")


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

    p_in = sub.add_parser("inbox", help="나에게 온 승인 요청 (기본은 결재 대기, --done 이면 결재가 끝난 것)")
    add_common(p_in)
    p_in.add_argument("--done", action="store_true", help="결재가 끝난 요청")
    p_in.add_argument("--limit", type=int, default=50)
    p_in.add_argument("--page", type=int, default=0, help="쪽 번호 (0 부터)")

    p_show = sub.add_parser("show", help="요청 한 건의 SQL, 사유, 결재 의견 (보낸 요청과 받은 요청 모두)")
    add_common(p_show)
    p_show.add_argument("uuid", help="요청 uuid (목록에서 각 요청 아래 줄에 찍힌 값)")

    p_ap = sub.add_parser("approve", help="받은 요청 승인 (먼저 --dry-run 으로 대상과 링크 비교를 확인)")
    add_common(p_ap)
    p_ap.add_argument("uuid", nargs="+", help="승인할 요청 uuid (inbox 에서 각 요청 아래 줄에 찍힌 값)")
    p_ap.add_argument("--comment", default=DEFAULT_APPROVE_COMMENT,
                      help=f"결재 의견 (기본: 웹 UI 의 기본 의견과 같은 '{DEFAULT_APPROVE_COMMENT}')")
    p_ap.add_argument("--dry-run", action="store_true",
                      help="보내지 않고 대상, 내 결재 차례인지, 링크 비교 결과만 출력")

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
            status = APPROVAL_STATUS.get(r["status"], str(r["status"]))
            kind = REQUEST_TYPE.get(r["type"], str(r["type"]))
            print(f'  #{r["number"]} [{status}] {kind}  {to_kst(r["created"])}  {r["title"]}')
            print(f'      {r["uuid"]}')
        return

    if args.cmd == "inbox":
        if args.limit < 1 or args.page < 0:
            ap.error("--limit 은 1 이상, --page 는 0 이상이어야 합니다")
        rows, total = list_received("GetDonePage" if args.done else "GetToDoPage",
                                    args.limit, args.page, args.insecure, window_id)
        print(f"  {'결재가 끝난 요청' if args.done else '결재 대기'} {total}건")
        first = args.page * args.limit
        if not rows and total:
            print("  (이 쪽에는 없습니다. 쪽 번호는 0 부터입니다)")
        for r in rows:
            status = APPROVAL_STATUS.get(r["status"], str(r["status"]))
            kind = REQUEST_TYPE.get(r["type"], str(r["type"]))
            urgent = " [긴급]" if r["urgent"] else ""
            print(f'  #{r["number"]} [{status}]{urgent} {kind}  {to_kst(r["created"])}  '
                  f'{r["requester"]}  {r["title"]}')
            print(f'      {r["uuid"]}')
        if rows and total > first + len(rows):
            print(f"  [주의] 전체 {total}건 중 {first + 1}번째부터 {len(rows)}건만 보였습니다. "
                  f"다음 쪽은 --page {args.page + 1}")
        return

    if args.cmd == "show":
        rtype = request_type(args.uuid, args.insecure, window_id)
        kind = DETAIL_KIND.get(rtype)
        if not kind:
            sys.exit(f"'{REQUEST_TYPE.get(rtype, rtype)}' 요청은 상세를 풀지 못합니다 "
                     "(쿼리 실행과 내보내기만 됩니다).")
        print_detail(args.uuid, kind, get_detail(args.uuid, kind, args.insecure, window_id))
        return

    if args.cmd == "approve":
        if not args.comment.strip():
            ap.error("--comment 가 비어 있습니다")
        approve_requests(args.uuid, args.comment, args.dry_run, args.insecure, window_id)
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
