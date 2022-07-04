-- noinspection SqlNoDataSourceInspectionForFile

/*

Pregunta
===========================================================================

Escriba una consulta que compute la cantidad de registros por letra de la 
columna 2 y clave de la columna 3 esto es, por ejemplo, la cantidad de
registros en tienen la letra `a` en la columna 2 y la clave `aaa` en la 
columna 3 es:

    a    aaa    5

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (
    c1 STRING,
    c2 ARRAY<CHAR(1)>, 
    c3 MAP<STRING, INT>
    )
    ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY '#'
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE t0;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS table_transform;
CREATE TABLE table_transform AS
    SELECT c2, key, value
    FROM t0 LATERAL VIEW EXPLODE(c3) t0 AS key, value;

DROP TABLE IF EXISTS table_target;
CREATE TABLE table_target AS
    SELECT letter, key, value
    FROM table_transform LATERAL VIEW EXPLODE(c2) table_transform AS letter;

INSERT OVERWRITE DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT letter, key, COUNT(1)
FROM table_target
GROUP BY letter, key;