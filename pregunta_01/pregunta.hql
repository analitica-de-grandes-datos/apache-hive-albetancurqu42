-- noinspection SqlNoDataSourceInspectionForFile

/*
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS data_source;
DROP TABLE IF EXISTS words_count;
CREATE TABLE data_source (key STRING, date_col DATETIME, number INT)

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'; -- TBLPROPERTIES ("skip.header.line.count"="0");

LOAD DATA LOCAL INPATH "./data.tsv" OVERWRITE INTO TABLE data_source;

CREATE TABLE words_count AS
    SELECT key, count(1) AS count
    FROM data_source
    GROUP BY key
    ORDER BY key;

INSERT OVERWRITE DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM words_count;
