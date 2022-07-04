/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Escriba una consulta que devuelva los cinco valores diferentes más pequeños 
de la tercera columna. ---> DISTINCT

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS table_source;
DROP TABLE IF EXISTS table_target;
CREATE TABLE table_source (key STRING, date_col DATE, number INT)

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH "./data.tsv" OVERWRITE INTO TABLE table_source;

CREATE TABLE table_target AS
    SELECT DISTINCT(number)
    FROM table_source
    ORDER BY number
    LIMIT 5;

INSERT OVERWRITE DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM table_target;
