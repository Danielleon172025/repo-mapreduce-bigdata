# Análisis de Big Data con Apache Hadoop y MapReduce

Actividad "Análisis de Big Data", Especialización en Analítica de Datos, Universidad de Cundinamarca.

Análisis con MapReduce (Java, Hadoop 3.2.1) del conjunto de datos abiertos "Oferta de Instituciones
Prestadoras de Servicios de Salud de la Red Pública de Bogotá D. C." (REPS), el mismo que se limpió con
Spark en la actividad anterior. El job cuenta, por cada clave, el total de sedes y cuántas declaran
atención 24 horas.

## Estructura

| Ruta | Contenido |
|---|---|
| `src/ConteoSedes.java` | Mapper, Combiner/Reducer y Driver del job MapReduce |
| `scripts/ejecutar_mapreduce.sh` | Compila, empaqueta y ejecuta los cuatro análisis dentro del contenedor `namenode` |
| `scripts/generar_figuras.py` | Genera las gráficas de `resultados/figuras/` con matplotlib a partir de los resultados |
| `data/osb_ofertasrv_sedes-red-publica-reps.csv` | Dataset de entrada (datos.gov.co) |
| `resultados/` | Salida de cada job (`resultado_*.txt`) y figuras (`figuras/`) |
| `logs/` | Log completo de cada job con los contadores de Hadoop |
| `docs/` | Informe técnico en PDF y DOCX, y capturas de pantalla en `docs/evidencias/` |

## Análisis (modos del job)

| Modo | Clave | Pregunta |
|---|---|---|
| `localidad` | código y nombre de la localidad | ¿Cómo se distribuye la oferta por localidad? |
| `subred` | subred integrada de servicios de salud | ¿Qué subred concentra más sedes y cuántas son 24 h? |
| `zona` | urbana / rural | ¿Qué proporción de la oferta es rural? |
| `nivel` | nivel de complejidad | Verificación de calidad del dato (el campo es constante) |

Salida de cada línea: `clave <TAB> total_sedes,sedes_24h`.

## Requisitos

- Docker Desktop
- Clúster HDFS del proyecto `Hadoop-spark-docker` (imágenes `bde2020/hadoop-namenode` y `hadoop-datanode`
  2.0.0-hadoop3.2.1-java8) con el CSV cargado en `/data/input/` de HDFS.

## Reproducir

```bash
# 1. Levantar HDFS
docker-compose up -d namenode datanode

# 2. Cargar el CSV en HDFS (si no está)
docker exec namenode hdfs dfs -mkdir -p /data/input
docker exec namenode hdfs dfs -put -f /tmp/data/osb_ofertasrv_sedes-red-publica-reps.csv /data/input/

# 3. Copiar este repositorio al contenedor y ejecutar
docker cp . namenode:/tmp/mr
docker exec namenode bash /tmp/mr/scripts/ejecutar_mapreduce.sh

# 4. Traer resultados y logs
docker cp namenode:/tmp/mr/resultados/. ./resultados/
docker cp namenode:/tmp/mr/logs/. ./logs/
```

Para regenerar las gráficas (requiere Python y matplotlib):

```bash
python scripts/generar_figuras.py .
```

Los jobs se ejecutan con `-D mapreduce.framework.name=local` (LocalJobRunner), porque el `docker-compose.yml`
del proyecto no incluye un NodeManager de YARN.

## Referencias

- Apache Software Foundation. (2023). *MapReduce tutorial* (Versión 3.3.5). Apache Hadoop.
  https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
- Holdsworth, J., & Kosinski, M. (s.f.). *¿Qué es MapReduce?* IBM.
  https://www.ibm.com/mx-es/think/topics/mapreduce
