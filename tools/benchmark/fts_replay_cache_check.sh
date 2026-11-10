#!/usr/bin/env bash
# 全文索引查询重放缓存的一致性回归检查。
#
# 反复执行同一条 MATCH 查询必须返回同样的行；提交任何写入之后，下一次执行
# 必须看到新的数据。脚本还会覆盖 MATCH(title, content) 和带 LIMIT 的
# top-k 两种扫描形态。
#
# 用法:
#   MYSQL='mysql -h127.0.0.1 -P2881 -uroot -N -s --default-character-set=utf8mb4' \
#     ./tools/benchmark/fts_replay_cache_check.sh

set -euo pipefail

MYSQL="${MYSQL:-mysql -h127.0.0.1 -P2881 -uroot -N -s --default-character-set=utf8mb4}"
DB="${DB:-fts_replay_check}"

query() {
  $MYSQL -e "SET NAMES utf8mb4; SET ob_query_timeout = 100000000000; $1" | tail -1
}

failures=0
check() {
  local name="$1" expected="$2" actual="$3"
  if [[ "${expected}" == "${actual}" ]]; then
    echo "  ok   ${name}: ${actual}"
  else
    echo "  FAIL ${name}: expected '${expected}' got '${actual}'"
    failures=$((failures + 1))
  fi
}

echo "=== FTS replay cache consistency check ==="

$MYSQL <<SQL
SET NAMES utf8mb4;
SET ob_query_timeout = 100000000000;
DROP DATABASE IF EXISTS ${DB};
CREATE DATABASE ${DB};
USE ${DB};
CREATE TABLE docs (
  id      BIGINT PRIMARY KEY,
  title   VARCHAR(256) NOT NULL,
  content TEXT NOT NULL
) DEFAULT CHARSET = utf8mb4;
INSERT INTO docs VALUES
  (1, 'doc one',   'replaycache alpha'),
  (2, 'doc two',   'replaycache beta'),
  (3, 'doc three', 'replaycache gamma'),
  (4, 'doc four',  'othertoken'),
  (5, 'doc five',  'plain text'),
  (6, 'titlesecret six', 'plain text'),
  (7, 'doc seven', '全文索引 分词器 doc');
ALTER TABLE docs ADD FULLTEXT INDEX fti_content(content) WITH PARSER ik;
ALTER TABLE docs ADD FULLTEXT INDEX fti_title_content(title, content) WITH PARSER ik;
SQL

content_count="SELECT COALESCE(GROUP_CONCAT(id ORDER BY id), '') FROM ${DB}.docs WHERE MATCH(content) AGAINST('replaycache');"
title_count="SELECT COALESCE(GROUP_CONCAT(id ORDER BY id), '') FROM ${DB}.docs WHERE MATCH(title, content) AGAINST('titlesecret');"
cn_count="SELECT COUNT(*) FROM ${DB}.docs WHERE MATCH(title, content) AGAINST('全文索引 分词器');"
limit_set="SELECT COALESCE(GROUP_CONCAT(id ORDER BY id), '') FROM (SELECT id FROM ${DB}.docs WHERE MATCH(content) AGAINST('replaycache') LIMIT 2) t;"
limit_count="SELECT COUNT(*) FROM (SELECT id FROM ${DB}.docs WHERE MATCH(content) AGAINST('replaycache') LIMIT 2) t;"

# 1. The same natural-language query must keep returning the same rows.
check "content initial" "1,2,3" "$(query "${content_count}")"
check "content repeat" "1,2,3" "$(query "${content_count}")"
for _ in $(seq 1 20); do
  actual=$(query "${content_count}")
  if [[ "${actual}" != "1,2,3" ]]; then
    check "content repeated round" "1,2,3" "${actual}"
    break
  fi
done

# 2. A committed insert must be visible on the very next execution.
query "INSERT INTO ${DB}.docs VALUES (8, 'doc eight', 'replaycache delta');" >/dev/null
check "content after insert" "1,2,3,8" "$(query "${content_count}")"
check "content after insert repeat" "1,2,3,8" "$(query "${content_count}")"

# 3. A committed update must be visible on the very next execution.
query "UPDATE ${DB}.docs SET content = 'plain text' WHERE id = 8;" >/dev/null
check "content after update" "1,2,3" "$(query "${content_count}")"

# 4. A committed delete must be visible on the very next execution.
query "DELETE FROM ${DB}.docs WHERE id = 2;" >/dev/null
check "content after delete" "1,3" "$(query "${content_count}")"

# 5. The two-column MATCH shape must follow committed changes too.
check "title initial" "6" "$(query "${title_count}")"
check "title repeat" "6" "$(query "${title_count}")"
query "UPDATE ${DB}.docs SET title = 'plain six' WHERE id = 6;" >/dev/null
check "title after update" "" "$(query "${title_count}")"

# 6. The pushed-down top-k LIMIT shape must return a stable set and drop a
#    deleted row from the next result.
query "INSERT INTO ${DB}.docs VALUES (9, 'doc nine', 'replaycache epsilon');" >/dev/null
first_set=$(query "${limit_set}")
check "limit initial size" "2" "$(query "${limit_count}")"
check "limit repeat" "${first_set}" "$(query "${limit_set}")"
victim="${first_set%%,*}"
query "DELETE FROM ${DB}.docs WHERE id = ${victim};" >/dev/null
check "limit drops deleted row" "0" \
  "$(query "SELECT COUNT(*) FROM (SELECT id FROM ${DB}.docs WHERE MATCH(content) AGAINST('replaycache') LIMIT 2) t WHERE id = ${victim};")"
check "limit after delete size" "2" "$(query "${limit_count}")"

# 7. The benchmark query shape: two-column natural-language MATCH.
check "benchmark shape initial" "1" "$(query "${cn_count}")"
check "benchmark shape repeat" "1" "$(query "${cn_count}")"
query "INSERT INTO ${DB}.docs VALUES (10, 'doc ten', '全文索引 分词器 重复');" >/dev/null
check "benchmark shape after insert" "2" "$(query "${cn_count}")"
check "benchmark shape after insert repeat" "2" "$(query "${cn_count}")"

if (( failures > 0 )); then
  echo "FAILED: ${failures} replay cache consistency check(s)"
  exit 1
fi

echo "PASS: replay cache stays consistent across repeats and commits"
