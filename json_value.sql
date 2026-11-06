-- JSON_VALUE end-to-end
--
-- Cases:
--   snk  : core semantics (12 rows = 6 shapes duplicated): lax/strict routing,
--          quoted key, null-at-path, invalid json, sql-null
--   snk2 : non-string DEFAULT literals (4 rows): DEFAULT FALSE/42/1.5
--   snk3 : edge cases (6 rows of heterogeneous documents): empty-string value,
--          root document, root array, numeric key, big integer, escaped default,
--          array out-of-bounds (strict/lax)
--   snk4 : RETURNING INT/BOOLEAN/DOUBLE + DEFAULT (2 rows)
--   snk5 : return-type comparison (3 rows): same JSON default VARCHAR vs
--          RETURNING INT/DOUBLE/BOOLEAN, incl. null-json propagation
--   snk6 : lax/strict non-scalar matrix (2 rows): object/array/JSON-null/missing
--          field routed to ON EMPTY under lax, ON ERROR under strict
--   snk7 : malformed-JSON lax/strict routing (3 rows): invalid json routes to
--          ON EMPTY under lax (aligned with Flink, diverging from the SQL
--          standard), ON ERROR under strict
--   snk8 : number formatting (1 row): integer beyond uint64 (BigInteger), 2^64,
--          non-canonical float spelling (1.50), scientific notation (1e3)
SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE TABLE snk (
  id          BIGINT,
  jv_name     STRING,
  jv_number   STRING,
  jv_bool     STRING,
  jv_nested   STRING,
  jv_array    STRING,
  jv_qdot     STRING,
  jv_qblank   STRING,
  jv_lax_def  STRING,
  jv_str_def  STRING,
  jv_null_def STRING
) WITH ('connector' = 'print');

INSERT INTO snk
SELECT id,
       JSON_VALUE(j, '$.name'),
       JSON_VALUE(j, '$.n'),
       JSON_VALUE(j, '$.b'),
       JSON_VALUE(j, '$.a.b'),
       JSON_VALUE(j, '$.arr[0]'),
       JSON_VALUE(j, '$.[''a.b''][0]'),
       JSON_VALUE(j, 'strict $.[''contains blank'']' NULL ON EMPTY DEFAULT 'wrong' ON ERROR),
       JSON_VALUE(j, 'lax $.missing' DEFAULT 'none' ON EMPTY),
       JSON_VALUE(j, 'strict $.missing' DEFAULT 'oops' ON ERROR),
       JSON_VALUE(j, 'strict $.name' DEFAULT 'err' ON ERROR)
FROM (
  VALUES
  (1,  CAST('{"name":"alice","n":42,"b":true,"a":{"b":2},"arr":[10,20],"a.b":[0.998,0.996],"contains blank":"right"}' AS STRING)),
  (2,  CAST('{"name":"bob","n":-7,"b":false,"a":{"b":5},"arr":[30,40],"a.b":[0.5,0.25],"contains blank":"right"}' AS STRING)),
  (3,  CAST('{"name":null,"n":7,"b":true,"a":{"b":2},"arr":[10,20],"a.b":[0.998,0.996],"contains blank":"right"}' AS STRING)),
  (4,  CAST('{}' AS STRING)),
  (5,  CAST('not-json' AS STRING)),
  (6,  CAST(NULL AS STRING)),
  (7,  CAST('{"name":"alice","n":42,"b":true,"a":{"b":2},"arr":[10,20],"a.b":[0.998,0.996],"contains blank":"right"}' AS STRING)),
  (8,  CAST('{"name":"bob","n":-7,"b":false,"a":{"b":5},"arr":[30,40],"a.b":[0.5,0.25],"contains blank":"right"}' AS STRING)),
  (9,  CAST('{"name":null,"n":7,"b":true,"a":{"b":2},"arr":[10,20],"a.b":[0.998,0.996],"contains blank":"right"}' AS STRING)),
  (10, CAST('{}' AS STRING)),
  (11, CAST('not-json' AS STRING)),
  (12, CAST(NULL AS STRING))
) AS src (id, j);

CREATE TABLE snk2 (
  id            BIGINT,
  jv_def_false_e STRING,
  jv_def_false_r STRING,
  jv_def_int     STRING,
  jv_def_dec     STRING
) WITH ('connector' = 'print');

INSERT INTO snk2
SELECT id,
       JSON_VALUE(j, 'lax $.missing' DEFAULT FALSE ON EMPTY),
       JSON_VALUE(j, 'strict $.missing' DEFAULT FALSE ON ERROR),
       JSON_VALUE(j, 'lax $.missing' DEFAULT 42 ON EMPTY),
       JSON_VALUE(j, 'lax $.missing' DEFAULT 1.5 ON EMPTY)
FROM (
  VALUES
  (1, CAST('{"name":"alice"}' AS STRING)),
  (2, CAST(NULL AS STRING)),
  (3, CAST('{"name":"alice"}' AS STRING)),
  (4, CAST(NULL AS STRING))
) AS src (id, j);

CREATE TABLE snk3 (
  id           BIGINT,
  jv_name      STRING,
  jv_root      STRING,
  jv_root0     STRING,
  jv_nested    STRING,
  jv_numkey    STRING,
  jv_num       STRING,
  jv_def_empty STRING,
  jv_def_quote STRING,
  jv_oob_str   STRING,
  jv_oob_lax   STRING
) WITH ('connector' = 'print');

INSERT INTO snk3
SELECT id,
       JSON_VALUE(j, '$.name'),
       JSON_VALUE(j, '$'),
       JSON_VALUE(j, '$[0]'),
       JSON_VALUE(j, '$.m[1][0]'),
       JSON_VALUE(j, '$.[''1'']'),
       JSON_VALUE(j, '$.n'),
       JSON_VALUE(j, 'lax $.missing' DEFAULT '' ON EMPTY),
       JSON_VALUE(j, 'strict $.missing' DEFAULT 'it''s "q"' ON ERROR),
       JSON_VALUE(j, 'strict $.m[5]' DEFAULT 'err' ON ERROR),
       JSON_VALUE(j, 'lax $.m[5]' DEFAULT 'e' ON EMPTY)
FROM (
  VALUES
  (1, CAST('{"name":"","n":"42","m":[[1,2],[3,4]]}' AS STRING)),
  (2, CAST('42' AS STRING)),
  (3, CAST('[10,20]' AS STRING)),
  (4, CAST('{"name":"\u0041lice"}' AS STRING)),
  (5, CAST('{ "name" : "spaced" , "n" : 7 }' AS STRING)),
  (6, CAST('{"name":"say \"hi\"","n":9007199254740993,"m":[[1,2],[3,4]],"1":"numkey"}' AS STRING))
) AS src (id, j);
CREATE TABLE snk4 (
  id         BIGINT,
  jv_int     INT,
  jv_bool    BOOLEAN,
  jv_double  DOUBLE,
  jv_int_def INT
) WITH ('connector' = 'print');

INSERT INTO snk4
SELECT id,
       JSON_VALUE(j, '$.n' RETURNING INT),
       JSON_VALUE(j, '$.b' RETURNING BOOLEAN),
       JSON_VALUE(j, '$.d' RETURNING DOUBLE),
       JSON_VALUE(j, 'lax $.missing' RETURNING INT DEFAULT 42 ON EMPTY)
FROM (
  VALUES
  (1, CAST('{"n":42,"b":true,"d":1.5}' AS STRING)),
  (2, CAST('{"n":7,"b":false,"d":2.5}' AS STRING))
) AS src (id, j);

CREATE TABLE snk5 (
  id          BIGINT,
  jv_n_str    STRING,
  jv_n_int    INT,
  jv_d_str    STRING,
  jv_d_double DOUBLE,
  jv_b_str    STRING,
  jv_b_bool   BOOLEAN
) WITH ('connector' = 'print');

INSERT INTO snk5
SELECT id,
       JSON_VALUE(j, '$.n'),
       JSON_VALUE(j, '$.n' RETURNING INT),
       JSON_VALUE(j, '$.d'),
       JSON_VALUE(j, '$.d' RETURNING DOUBLE),
       JSON_VALUE(j, '$.b'),
       JSON_VALUE(j, '$.b' RETURNING BOOLEAN)
FROM (
  VALUES
  (1, CAST('{"n":42,"d":1.5,"b":true}' AS STRING)),
  (2, CAST('{"n":7,"d":2.75,"b":false}' AS STRING)),
  (3, CAST(NULL AS STRING))
) AS src (id, j);

CREATE TABLE snk6 (
  id          BIGINT,
  jv_lax_obj  STRING,
  jv_str_obj  STRING,
  jv_lax_arr  STRING,
  jv_str_arr  STRING,
  jv_lax_null STRING,
  jv_str_null STRING,
  jv_lax_miss STRING,
  jv_str_miss STRING
) WITH ('connector' = 'print');

INSERT INTO snk6
SELECT id,
       JSON_VALUE(j, 'lax $.obj' DEFAULT 'e' ON EMPTY),
       JSON_VALUE(j, 'strict $.obj' DEFAULT 's' ON ERROR),
       JSON_VALUE(j, 'lax $.arr' DEFAULT 'e' ON EMPTY),
       JSON_VALUE(j, 'strict $.arr' DEFAULT 's' ON ERROR),
       JSON_VALUE(j, 'lax $.x' DEFAULT 'e' ON EMPTY),
       JSON_VALUE(j, 'strict $.x' DEFAULT 's' ON ERROR),
       JSON_VALUE(j, 'lax $.missing' DEFAULT 'e' ON EMPTY),
       JSON_VALUE(j, 'strict $.missing' DEFAULT 's' ON ERROR)
FROM (
  VALUES
  (1, CAST('{"obj":{},"arr":[1,2],"x":null}' AS STRING)),
  (2, CAST('{"name":"alice"}' AS STRING))
) AS src (id, j);

CREATE TABLE snk7 (
  id     BIGINT,
  jv_lax STRING,
  jv_str STRING
) WITH ('connector' = 'print');

INSERT INTO snk7
SELECT id,
       JSON_VALUE(j, 'lax $.a' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR),
       JSON_VALUE(j, 'strict $.a' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR)
FROM (
  VALUES
  (1, CAST('not-json' AS STRING)),
  (2, CAST('{"a":1}' AS STRING)),
  (3, CAST(NULL AS STRING))
) AS src (id, j);

CREATE TABLE snk8 (
  id       BIGINT,
  jv_big   STRING,
  jv_p64   STRING,
  jv_float STRING,
  jv_sci   STRING
) WITH ('connector' = 'print');

INSERT INTO snk8
SELECT id,
       JSON_VALUE(j, '$.big'),
       JSON_VALUE(j, '$.p64'),
       JSON_VALUE(j, '$.f'),
       JSON_VALUE(j, '$.s')
FROM (
  VALUES
  (1, CAST('{"big":123456789012345678901234567890,"p64":18446744073709551616,"f":1.50,"s":1e3}' AS STRING))
) AS src (id, j);
