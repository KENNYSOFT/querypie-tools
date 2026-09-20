# querypie-tools

QueryPie 웹 UI 가 쓰는 gRPC-Web 인터페이스를 그대로 호출하는 CLI 도구 모음입니다. 파이썬 표준 라이브러리만 씁니다.

프록시의 `--experimental-ddl` 만 `sqlglot` 을 쓰고, 그것도 없으면 그 기능만 꺼집니다. 쓰려면 저장소 옆에 받아 둡니다.

```bash
python -m pip install --target vendor sqlglot
```

할 수 있는 일은 계정 권한 안에서만입니다. 쿼리는 QueryPie 를 그대로 거치므로 접근 제어와 감사 로그는 서버 정책을 따릅니다.

## 도구

| 도구 | 하는 일 |
|---|---|
| `querypie_query.py` | SQL 조회. 결과를 표나 TSV 로 출력 |
| `querypie_proxy.py` | 로컬 MySQL 프록시. HeidiSQL 같은 GUI 클라이언트를 QueryPie 에 붙인다 |
| `querypie_conn.py` | 커넥션 목록 조회와 database 매핑 관리 |
| `querypie_request.py` | SQL 실행 승인 요청 제출과 조회 |
| `querypie_import_har.py` | HAR 에서 세션 페이로드 추출 (커넥션 조립이 실패할 때의 대비책) |

각 파일 맨 위 docstring 에 사용법이 있습니다.

## 설정

설정과 자격은 `~/.querypie/` 에 둡니다. 다른 위치를 쓰려면 `QUERYPIE_HOME` 환경변수로 지정합니다.

`~/.querypie/querypie-config.json` 을 직접 만듭니다.

```json
{
  "host": "querypie.example.com",
  "default_database": "shop",
  "default_connection": "shop"
}
```

- `host` (필수) — QueryPie 웹 UI 주소에서 호스트만 적습니다. 기본값을 두지 않으므로 이 파일이 없으면 도구가 그 자리에서 멈춥니다.
- `default_database` (선택) — 조회에서 `--db` 를 생략했을 때 쓸 database.
- `default_connection` (선택) — 프록시에서 `--conn-name` 을 생략했을 때 쓸 커넥션.

## 인증

`~/.querypie/querypie-login.json` 에 계정을 두면 쿠키가 없거나 만료됐을 때 도구가 알아서 로그인하고 갱신합니다.

```json
{"username": "...", "password": "..."}
```

쿠키는 `~/.querypie/querypie-cookie.json` 에 도구가 만들고 갱신합니다. 자격 값은 어느 경로로도 stdout 에 출력되지 않습니다.

로그인 파일 없이 쓰려면 브라우저에서 쿠키를 복사해 같은 파일에 직접 넣어도 됩니다.

```json
{"cookies": {"qp_access_token": "...", "qp_refresh_token": "..."}}
```

access token 은 발급 후 20분 남짓이고 refresh 는 rotation 이라, 브라우저와 같은 refresh token 을 공유하면 먼저 호출한 쪽이 상대를 무효화합니다. 복사할 때 두 값을 함께 가져오세요.

## 커넥션

커넥션은 이름만 주면 그 자리에서 조립합니다. 이름은 클러스터 이름의 앞부분이면 됩니다(`shop-cluster` 면 `shop`). 그 클러스터의 엔드포인트를 골라 세션을 열기 때문에, 노드가 교체돼도 그대로 붙습니다.

```bash
python querypie_conn.py list          # 후보와 자동 선택 대상 확인
python querypie_conn.py map           # database -> 커넥션 매핑 갱신
```

매핑을 채워 두면 `--db <database>` 만으로 커넥션이 정해집니다.

조회는 읽기 엔드포인트, 승인 요청은 쓰기 엔드포인트로 붙습니다. 조회에서도 복제 지연 없이 방금 바뀐 값을 봐야 하면 `--writer` 를 씁니다.

## 프록시

```bash
python querypie_proxy.py --port 3307
```

HeidiSQL 에서 `127.0.0.1:3307` 로 접속합니다. 기본은 읽기 전용이고 `--allow-write` 로 풉니다. 사용자와 암호는 검사하지 않지만, `--login-from-client` 를 켜면 접속할 때 입력한 계정으로 QueryPie 에 로그인하고 그 값을 메모리에만 둡니다.

테이블 목록이나 DDL 같은 개체 탐색은 QueryPie 의 dictionary API 로 대신 처리해 SQL 왕복보다 10배쯤 빠릅니다.
