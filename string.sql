-- FROM_BASE64 end-to-end
--
-- Covers 8 base64 shapes (each duplicated under a fresh id) + 1 long input:
--   padded, unpadded, empty string, null, multibyte UTF-8,
--   1/2/3-byte padding progression (A / AB / ABC), and a 100-byte long payload.
-- Pure projection (FROM_BASE64 returns VARCHAR, aligned with Flink STRING).
SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TABLE snk (
  id      BIGINT,
  decoded STRING
) WITH ('connector' = 'print');

INSERT INTO snk
SELECT id,
       FROM_BASE64(b64)
FROM (
  VALUES
  (1,  CAST('dGVzdA==' AS STRING)),
  (2,  CAST('dGVzdA' AS STRING)),
  (3,  CAST('' AS STRING)),
  (4,  CAST(NULL AS STRING)),
  (5,  CAST('5L2g5aW9' AS STRING)),
  (6,  CAST('QQ==' AS STRING)),
  (7,  CAST('QUE=' AS STRING)),
  (8,  CAST('QUJD' AS STRING)),
  (9,  CAST('dGVzdA==' AS STRING)),
  (10, CAST('dGVzdA' AS STRING)),
  (11, CAST('' AS STRING)),
  (12, CAST(NULL AS STRING)),
  (13, CAST('5L2g5aW9' AS STRING)),
  (14, CAST('QQ==' AS STRING)),
  (15, CAST('QUE=' AS STRING)),
  (16, CAST('QUJD' AS STRING)),
  (17, CAST('YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYQ==' AS STRING))
) AS src (id, b64);

-- LIKE end-to-end (aligned with Flink sqlToRegexLike)
--
--   snk  : main test (16 rows) — dynamic pattern column + fixed literal forms + NULL matrix
--   snk2 : filter test (WHERE s LIKE 'a%') — filter side of FilterProject
--   snk3 : line-terminator test — `_` does not match newline, `%` matches newline
SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TABLE snk (
  id        BIGINT,
  s         STRING,
  lk_dyn    BOOLEAN,
  lk_prefix BOOLEAN,
  lk_suffix BOOLEAN,
  lk_cont   BOOLEAN,
  lk_single BOOLEAN,
  lk_exact  BOOLEAN,
  lk_case   BOOLEAN,
  lk_mb     BOOLEAN,
  lk_bs     BOOLEAN,
  lk_empty  BOOLEAN
) WITH ('connector' = 'print');

INSERT INTO snk
SELECT id, s,
       s LIKE p,
       s LIKE 'ab%',
       s LIKE '%bc',
       s LIKE '%b%',
       s LIKE '_b_',
       s LIKE 'abc',
       s LIKE 'ABC',
       s LIKE '__',
       s LIKE 'a\b',
       s LIKE ''
FROM (
  VALUES
  (1,  CAST('abc' AS STRING),   CAST('a%' AS STRING)),
  (2,  CAST('abc' AS STRING),   CAST('%c' AS STRING)),
  (3,  CAST('abc' AS STRING),   CAST('b%' AS STRING)),
  (4,  CAST('abc' AS STRING),   CAST('a_c' AS STRING)),
  (5,  CAST('' AS STRING),      CAST('' AS STRING)),
  (6,  CAST('' AS STRING),      CAST('%' AS STRING)),
  (7,  CAST('' AS STRING),      CAST('_' AS STRING)),
  (8,  CAST('你好' AS STRING),  CAST('__' AS STRING)),
  (9,  CAST('你好' AS STRING),  CAST('你_' AS STRING)),
  (10, CAST('你好' AS STRING),  CAST('_好' AS STRING)),
  (11, CAST('a\b' AS STRING),   CAST('a\b' AS STRING)),
  (12, CAST('abc' AS STRING),   CAST('ABC' AS STRING)),
  (13, CAST(NULL AS STRING),    CAST('a%' AS STRING)),
  (14, CAST('abc' AS STRING),   CAST(NULL AS STRING)),
  (15, CAST('aXbXc' AS STRING), CAST('%X%' AS STRING)),
  (16, CAST('a%c' AS STRING),   CAST('a%c' AS STRING)),
  (17, CAST('abc' AS STRING),   CAST('' AS STRING))
) AS src (id, s, p);

CREATE TABLE snk2 (
  id BIGINT,
  s  STRING
) WITH ('connector' = 'print');

INSERT INTO snk2
SELECT id, s
FROM (
  VALUES
  (1,  CAST('abc' AS STRING)),
  (2,  CAST('abd' AS STRING)),
  (3,  CAST('bcd' AS STRING)),
  (4,  CAST('a\b' AS STRING)),
  (5,  CAST('' AS STRING)),
  (6,  CAST(NULL AS STRING)),
  (7,  CAST('你好' AS STRING))
) AS src (id, s)
WHERE s LIKE 'a%';

CREATE TABLE snk3 (
  id        BIGINT,
  lk_und    BOOLEAN,   -- s LIKE '_'
  lk_pct    BOOLEAN,   -- s LIKE '%'
  lk_a_und  BOOLEAN,   -- s LIKE 'a_b'
  lk_a_pct  BOOLEAN    -- s LIKE 'a%b'
) WITH ('connector' = 'print');

INSERT INTO snk3
SELECT id, s LIKE '_', s LIKE '%', s LIKE 'a_b', s LIKE 'a%b'
FROM (
  VALUES
  (1, CAST(U&'\000A'   AS STRING)),   -- LF
  (2, CAST(U&'\000D'   AS STRING)),   -- CR
  (3, CAST(U&'\0085'   AS STRING)),   -- NEL
  (4, CAST(U&'\2028'   AS STRING)),   -- LS
  (5, CAST(U&'\2029'   AS STRING)),   -- PS
  (6, CAST(U&'a\000Ab' AS STRING)),   -- a + LF + b
  (7, CAST('aXb'       AS STRING))    -- control: no line terminator
) AS src (id, s);

-- LIKE empty-pattern end-to-end (native vs gluten)
--
-- Verify the real semantics of an empty pattern:
--   const_like = s LIKE ''   (constant empty pattern)
--   dyn_like   = s LIKE p    (dynamic empty pattern, bypassing planner constant folding)
-- Also includes one 'a%' row as a control and one null row to verify null propagation.
SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TABLE snk (
  id         BIGINT,
  s          STRING,
  p          STRING,
  const_like BOOLEAN,
  dyn_like   BOOLEAN
) WITH ('connector' = 'print');

INSERT INTO snk
SELECT id, s, p, s LIKE '', s LIKE p
FROM (
  VALUES
  (1, CAST('abc' AS STRING), CAST('' AS STRING)),
  (2, CAST('' AS STRING), CAST('' AS STRING)),
  (3, CAST('abc' AS STRING), CAST('a%' AS STRING)),
  (4, CAST(NULL AS STRING), CAST('' AS STRING))
) AS src (id, s, p);

-- REPLACE end-to-end
--
-- Covers two groups:
--   snk  : null matrix (16 rows) x 6 REPLACE shapes (column-driven 3-arg, empty old inserts,
--          empty new deletes, no match, empty input, null propagation)
--   snk2 : 12 boundary shapes (overlapping scan, growth without re-scan, identity, whole string,
--          old longer than input, case, string head/tail, empty+empty, all empty, multibyte old/new)
SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TABLE snk (
  id              BIGINT,
  rep_basic       STRING,
  rep_empty_old   STRING,
  rep_delete      STRING,
  rep_no_match    STRING,
  rep_empty_input STRING
) WITH ('connector' = 'print');

INSERT INTO snk
SELECT id,
       REPLACE(s, old_str, new_str),
       REPLACE(s, '', 'x'),
       REPLACE(s, 'b', ''),
       REPLACE(s, 'zzz', 'Q'),
       REPLACE(CAST('' AS STRING), old_str, new_str)
FROM (
  VALUES
  (1,  CAST('abc' AS STRING), CAST('b' AS STRING),    CAST('X' AS STRING)),
  (2,  CAST('abc' AS STRING), CAST('b' AS STRING),    CAST(NULL AS STRING)),
  (3,  CAST('abc' AS STRING), CAST(NULL AS STRING),   CAST('X' AS STRING)),
  (4,  CAST('abc' AS STRING), CAST(NULL AS STRING),   CAST(NULL AS STRING)),
  (5,  CAST(NULL AS STRING),  CAST('b' AS STRING),    CAST('X' AS STRING)),
  (6,  CAST(NULL AS STRING),  CAST('b' AS STRING),    CAST(NULL AS STRING)),
  (7,  CAST(NULL AS STRING),  CAST(NULL AS STRING),   CAST('X' AS STRING)),
  (8,  CAST(NULL AS STRING),  CAST(NULL AS STRING),   CAST(NULL AS STRING)),
  (9,  CAST('abc' AS STRING), CAST('' AS STRING),     CAST('x' AS STRING)),
  (10, CAST('abc' AS STRING), CAST('' AS STRING),     CAST(NULL AS STRING)),
  (11, CAST('abc' AS STRING), CAST(NULL AS STRING),   CAST('x' AS STRING)),
  (12, CAST('abc' AS STRING), CAST(NULL AS STRING),   CAST(NULL AS STRING)),
  (13, CAST(NULL AS STRING),  CAST('' AS STRING),     CAST('x' AS STRING)),
  (14, CAST(NULL AS STRING),  CAST('' AS STRING),     CAST(NULL AS STRING)),
  (15, CAST(NULL AS STRING),  CAST(NULL AS STRING),   CAST('x' AS STRING)),
  (16, CAST(NULL AS STRING),  CAST(NULL AS STRING),   CAST(NULL AS STRING))
) AS src (id, s, old_str, new_str);

CREATE TABLE snk2 (
  id       BIGINT,
  replaced STRING
) WITH ('connector' = 'print');

INSERT INTO snk2
SELECT id, REPLACE(s, old_str, new_str)
FROM (
  VALUES
  (1,  CAST('aaa' AS STRING),    CAST('aa' AS STRING),  CAST('X' AS STRING)),
  (2,  CAST('aaa' AS STRING),    CAST('a' AS STRING),   CAST('aa' AS STRING)),
  (3,  CAST('abc' AS STRING),    CAST('b' AS STRING),   CAST('b' AS STRING)),
  (4,  CAST('abc' AS STRING),    CAST('abc' AS STRING), CAST('XYZ' AS STRING)),
  (5,  CAST('ab' AS STRING),     CAST('abc' AS STRING), CAST('X' AS STRING)),
  (6,  CAST('aAa' AS STRING),    CAST('a' AS STRING),   CAST('X' AS STRING)),
  (7,  CAST('abc' AS STRING),    CAST('a' AS STRING),   CAST('X' AS STRING)),
  (8,  CAST('abc' AS STRING),    CAST('c' AS STRING),   CAST('X' AS STRING)),
  (9,  CAST('abc' AS STRING),    CAST('' AS STRING),    CAST('' AS STRING)),
  (10, CAST('' AS STRING),       CAST('' AS STRING),    CAST('' AS STRING)),
  (11, CAST('abc你好' AS STRING), CAST('你好' AS STRING), CAST('de' AS STRING)),
  (12, CAST('abc' AS STRING),     CAST('b' AS STRING),   CAST('你好' AS STRING))
) AS src (id, s, old_str, new_str);

-- RIGHT end-to-end
--
-- For each string s, evaluate six values of n (-1/0/1/2/=len/>len) and project six columns.
-- Semantic anchors (Flink generateRight): n<=0 -> '', n>=char count -> whole string,
-- otherwise -> the last n UTF-8 code points; if any argument is NULL -> NULL.
SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TABLE snk (
  id      BIGINT,
  s       STRING,
  rt_neg1 STRING,
  rt_0    STRING,
  rt_1    STRING,
  rt_2    STRING,
  rt_eq   STRING,
  rt_gt   STRING
) WITH ('connector' = 'print');

INSERT INTO snk
SELECT id, s,
       RIGHT(s, -1),
       RIGHT(s, 0),
       RIGHT(s, 1),
       RIGHT(s, 2),
       RIGHT(s, n_eq),
       RIGHT(s, n_gt)
FROM (
  VALUES
  (1, CAST('abc'     AS STRING), CAST(3 AS INT), CAST(4 AS INT)),
  (2, CAST('hello'   AS STRING), CAST(5 AS INT), CAST(6 AS INT)),
  (3, CAST(''        AS STRING), CAST(0 AS INT), CAST(1 AS INT)),
  (4, CAST('abc你好' AS STRING), CAST(5 AS INT), CAST(6 AS INT)),
  (5, CAST(NULL      AS STRING), CAST(NULL AS INT), CAST(NULL AS INT)),
  (6, CAST('abc'     AS STRING), CAST(NULL AS INT), CAST(4 AS INT))
) AS src (id, s, n_eq, n_gt);
