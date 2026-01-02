import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucketRaw"])
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ===========================================
# RUTAS
# ===========================================
source = "drupal"
bucket = args["bucketRaw"]
staging_path = f"s3://{bucket}/{source}/staging/"
base_output = f"s3://{bucket}/{source}/consume/"
dbName = "tramites"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {source}")

# LECTURA
df_base = spark.read.parquet(staging_path + "tramites_base/")
df_docs = spark.read.parquet(staging_path + "documentos/")
df_pasos = spark.read.parquet(staging_path + "pasos/")
df_reqs = spark.read.parquet(staging_path + "requisitos_norm/") 
df_part = spark.read.parquet(staging_path + "participantes/")
df_costos = spark.read.parquet(staging_path + "costos/")
df_deps = spark.read.parquet(staging_path + "dependencias/")
df_marco = spark.read.parquet(staging_path + "marco_normativo/")
df_tipo = spark.read.parquet(staging_path + "tipo_tramite/")

current_ts = F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")
def add_audit(df): return df.withColumn("insert_into_dl_timestamp", F.lit(current_ts))

def save_table(df, table_name, subfolder):
    path = f"{base_output}{subfolder}/"
    add_audit(df).write.mode("overwrite").format("parquet").option("path", path).saveAsTable(f"{source}.{table_name}")

# ==============================================================================
# TIPO TRAMITE
# ==============================================================================
save_table(df_tipo, "tipo_tramite", "tipo_tramite")

# ==============================================================================
# PARTICIPANTES
# ==============================================================================
df_dim_part = df_part.select("NOMBRE_PARTICIPANTE") \
    .filter((F.col("NOMBRE_PARTICIPANTE").isNotNull()) & (F.length("NOMBRE_PARTICIPANTE") > 1)) \
    .dropDuplicates() \
    .withColumn("ID_PARTICIPANTE", F.monotonically_increasing_id()) \
    .withColumn("DESCRIPCION_PARTICIPANTE", F.concat(F.lit("Rol: "), F.col("NOMBRE_PARTICIPANTE"))) \
    .withColumn("ES_PERSONA_FISICA", F.when(F.lower(F.col("NOMBRE_PARTICIPANTE")).rlike("ciudadano|propietario|inquilino|profesional|tercero|visualizador"), True).otherwise(False)) \
    .withColumn("ES_PROFESIONAL", F.when(F.lower(F.col("NOMBRE_PARTICIPANTE")).rlike("profesional|profesional matriculado"), True).otherwise(False))

save_table(df_dim_part, "participantes", "participantes")

df_fact_part = df_part.join(df_dim_part, "NOMBRE_PARTICIPANTE").select("ID_TRAMITE", "ID_PARTICIPANTE").dropDuplicates()
save_table(df_fact_part, "tramites_participantes", "tramites_participantes")

# ==============================================================================
# DOCUMENTOS
# ==============================================================================
df_dim_docs = df_docs.select("NOMBRE_DOCUMENTO", "DESCRIPCION_DOCUMENTO", "DURACION_VALIDEZ", "TIENE_MODELO", "LINK_MODELO", "TIPO_DOCUMENTO") \
    .withColumn("DESCRIPCION_DOCUMENTO", F.coalesce(F.col("DESCRIPCION_DOCUMENTO"), F.col("NOMBRE_DOCUMENTO"))) \
    .withColumn("DURACION_VALIDEZ", F.col("DURACION_VALIDEZ").cast(IntegerType())) \
    .dropDuplicates(["NOMBRE_DOCUMENTO", "TIPO_DOCUMENTO", "LINK_MODELO"]) \
    .withColumn("ID_DOCUMENTO", F.monotonically_increasing_id())

save_table(df_dim_docs, "documentos", "documentos")
df_fact_docs_temp = df_docs.alias("raw").join(df_dim_docs.alias("dim"), ["NOMBRE_DOCUMENTO", "LINK_MODELO", "TIPO_DOCUMENTO"]) \
    .select(F.col("raw.ID_TRAMITE"), F.col("raw.ID_CONTENT_RAW"), F.col("dim.ID_DOCUMENTO")).dropDuplicates()

save_table(df_fact_docs_temp.drop("ID_CONTENT_RAW"), "tramites_documentos", "tramites_documentos")

df_fact_docs = df_docs.alias("raw").join(df_dim_docs.alias("dim"), ["NOMBRE_DOCUMENTO", "LINK_MODELO", "TIPO_DOCUMENTO"]) \
    .select(F.col("raw.ID_TRAMITE"), F.col("dim.ID_DOCUMENTO")).dropDuplicates()
save_table(df_fact_docs, "tramites_documentos", "tramites_documentos")

# ==============================================================================
# REQUISITOS
# ==============================================================================
df_reqs_master = df_reqs.select("NOMBRE_REQUISITO", "OBLIGATORIEDAD") \
    .withColumn("DESCRIPCION_REQUISITO", F.col("NOMBRE_REQUISITO")) \
    .dropDuplicates(["NOMBRE_REQUISITO", "OBLIGATORIEDAD"]) \
    .withColumn("ID_REQUISITO", F.md5(F.concat(F.col("NOMBRE_REQUISITO"), F.col("OBLIGATORIEDAD")))) \
    .withColumn("OBLIGATORIEDAD", F.when(F.col("OBLIGATORIEDAD") == "Y", True).otherwise(False))

save_table(df_reqs_master, "requisitos", "requisitos")

df_reqs_tramite = df_reqs.join(
        df_reqs_master.withColumn("OBL_BOOL_JOIN", F.col("OBLIGATORIEDAD")), 
        (df_reqs.NOMBRE_REQUISITO == df_reqs_master.NOMBRE_REQUISITO) & 
        (F.when(df_reqs.OBLIGATORIEDAD == "Y", True).otherwise(False) == F.col("OBL_BOOL_JOIN"))
    ).select("ID_TRAMITE", "ID_CONTENT_RAW", "ID_REQUISITO", "PERSONA_FISICA", "ID_SUBGRUPO") \
    .withColumn("PERSONA_FISICA", F.when(F.col("PERSONA_FISICA") == "Y", True).otherwise(False)) \
    .withColumn("ID_REQUISITO_TRAMITE", F.monotonically_increasing_id().cast(StringType()))

save_table(df_reqs_tramite.drop("ID_CONTENT_RAW"), "requisitos_tramites", "requisitos_tramites")

# ==============================================================================
# NUEVA RELACION: REQUISITOS - DOCUMENTOS
# ==============================================================================
df_req_doc_bridge = df_reqs_tramite.alias("req").join(
    df_fact_docs_temp.alias("doc"),
    (F.col("req.ID_TRAMITE") == F.col("doc.ID_TRAMITE")) & 
    (F.col("req.ID_CONTENT_RAW") == F.col("doc.ID_CONTENT_RAW"))
).select(
    F.col("req.ID_REQUISITO_TRAMITE"),
    F.col("doc.ID_DOCUMENTO")
).dropDuplicates()

save_table(df_req_doc_bridge, "requisitos_tramites_documentos", "requisitos_tramites_documentos")

# ==============================================================================
# PASOS (Con Orden e Interviniente)
# ==============================================================================
df_dim_pasos = df_pasos.select(
    F.col("TITULO_PASO_RAW").alias("TITULO_PASO"),
    F.col("DESCRIPCION_PASO_CLEAN").alias("DESCRIPCION_PASO")
).withColumn("TITULO_PASO", F.when((F.col("TITULO_PASO").isNull()) | (F.col("TITULO_PASO") == ""), F.lit("Paso del Trámite")).otherwise(F.col("TITULO_PASO"))) \
 .withColumn("DESCRIPCION_PASO", F.when(F.col("DESCRIPCION_PASO").isNull(), F.lit("Sin descripción")).otherwise(F.col("DESCRIPCION_PASO"))) \
 .dropDuplicates(["TITULO_PASO", "DESCRIPCION_PASO"]) \
 .withColumn("ID_PASO", F.monotonically_increasing_id())

save_table(df_dim_pasos, "pasos", "pasos")

df_fact_pasos = df_pasos.alias("raw").join(df_dim_pasos.alias("dim"), 
    (F.col("raw.TITULO_PASO_RAW") == F.col("dim.TITULO_PASO")) & (F.col("raw.DESCRIPCION_PASO_CLEAN") == F.col("dim.DESCRIPCION_PASO"))
).select(
    F.col("raw.ID_TRAMITE"), 
    F.col("dim.ID_PASO"), 
    F.col("raw.ORDEN_PASO"),
    F.col("raw.INTERVINIENTE"),
    F.col("raw.MOSTRAR_EN_PLATAFORMA")
).dropDuplicates()

save_table(df_fact_pasos, "tramites_pasos", "tramites_pasos")

# Dim Plataforma
df_plat_web = df_pasos.filter(F.col("NOMBRE_PLATAFORMA").isNotNull()) \
    .select(
        F.col("NOMBRE_PLATAFORMA").alias("NOMBRE_PLATAFORMA_DE_TRAMITACION"),
        F.col("LINK_PLATAFORMA").alias("LINK_AL_INICIO_DEL_TRAMITE")
    ).dropDuplicates() \
    .withColumn("MODALIDAD", F.lit("Web/Online")) \
    .withColumn("DESCRIPCION_PLATAFORMA_DE_TRAMITACION", F.concat(F.lit("Plataforma: "), F.col("NOMBRE_PLATAFORMA_DE_TRAMITACION")))

schema_presencial = StructType([
    StructField("NOMBRE_PLATAFORMA_DE_TRAMITACION", StringType(), True),
    StructField("LINK_AL_INICIO_DEL_TRAMITE", StringType(), True),
    StructField("MODALIDAD", StringType(), True),
    StructField("DESCRIPCION_PLATAFORMA_DE_TRAMITACION", StringType(), True)
])

df_plat_pres = spark.createDataFrame(
    data=[("Presencial", None, "Presencial", "Trámite presencial")], 
    schema=schema_presencial
)
df_dim_plat = df_plat_web.unionByName(df_plat_pres, allowMissingColumns=True) \
    .dropDuplicates(["NOMBRE_PLATAFORMA_DE_TRAMITACION"]) \
    .withColumn("ID_PLATAFORMA_DE_TRAMITACION", F.monotonically_increasing_id())

save_table(df_dim_plat, "plataforma_de_tramitacion", "plataforma_de_tramitacion")

# Fact Plataforma
df_fact_plat_web = df_pasos.alias("p").join(df_dim_plat.alias("d"), 
        F.col("p.NOMBRE_PLATAFORMA") == F.col("d.NOMBRE_PLATAFORMA_DE_TRAMITACION")
    ).select(F.col("p.ID_TRAMITE"), F.col("d.ID_PLATAFORMA_DE_TRAMITACION"))

id_presencial_row = df_dim_plat.filter(F.col("NOMBRE_PLATAFORMA_DE_TRAMITACION") == "Presencial").select("ID_PLATAFORMA_DE_TRAMITACION").first()

if id_presencial_row:
    id_p = id_presencial_row[0]
    df_fact_plat_pres = df_base.withColumn("mod", F.explode_outer("MODALIDAD_IDS")) \
        .filter(F.col("mod") == "21717") \
        .select(F.col("CODIGO_TRAMITE").alias("ID_TRAMITE"), F.lit(id_p).alias("ID_PLATAFORMA_DE_TRAMITACION"))
    df_fact_plat = df_fact_plat_web.union(df_fact_plat_pres).dropDuplicates()
else: df_fact_plat = df_fact_plat_web

save_table(df_fact_plat, "tramites_plataforma", "tramites_plataforma")

# ==============================================================================
# MEDIOS PAGO
# ==============================================================================
df_mp_raw = df_base.select("CODIGO_TRAMITE", F.explode_outer("MEDIOS_PAGO_ARRAY").alias("NOMBRE_MEDIO_PAGO")).filter(F.col("NOMBRE_MEDIO_PAGO").isNotNull())
df_dim_mp = df_mp_raw.select("NOMBRE_MEDIO_PAGO").dropDuplicates().withColumn("ID_MEDIO_PAGO", F.monotonically_increasing_id()) \
    .withColumn("DESCRIPCION", F.lit("Medio de pago habilitado")) # Fix descripción
save_table(df_dim_mp, "medio_pago", "medio_pago")

df_fact_mp = df_mp_raw.join(df_dim_mp, "NOMBRE_MEDIO_PAGO").select(F.col("CODIGO_TRAMITE").alias("ID_TRAMITE"), "ID_MEDIO_PAGO").dropDuplicates()
save_table(df_fact_mp, "tramites_medio_pago", "tramites_medio_pago")

# ==============================================================================
# DEPENDENCIAS
# ==============================================================================
save_table(df_deps, "dependencias", "dependencias")

# ==============================================================================
# TABLA PRINCIPAL TRAMITES
# ==============================================================================
# Costos aggregation (Lista de precios por trAmite)
df_costos_agg = df_costos.withColumn("CONCEPTO", F.coalesce(F.col("CONCEPTO"), F.lit("General"))) \
    .withColumn("PRECIO_DEC", F.regexp_replace("PRECIO_STR", "[^0-9.]", "").cast("decimal(10,2)")) \
    .groupBy("ID_TRAMITE").agg(
        F.collect_list("PRECIO_DEC").alias("MONTO_COSTO"), 
        F.collect_list("CONCEPTO").alias("CONCEPTO_PAGO"), 
        F.max("PRECIO_DEC").alias("MAX_COSTO")
    ).withColumn("REQUIERE_PAGO", F.when(F.col("MAX_COSTO") > 0, "Y").otherwise("N"))

df_marco_agg = df_marco.filter(F.lower(F.col("BODY_CLEAN")).rlike("ley%20tarifaria")).groupBy("ID_TRAMITE").agg(F.max(F.lit("Se encontró Ley Tarifaria")).alias("MARCO_NORMATIVO"))
df_turno_agg = df_pasos.filter(F.col("CANAL_TURNO").isNotNull()).groupBy("ID_TRAMITE").agg(F.first("CANAL_TURNO").alias("CANAL_TURNO"))

df_flat = df_base.join(df_costos_agg.withColumnRenamed("ID_TRAMITE", "CODIGO_TRAMITE"), "CODIGO_TRAMITE", "left") \
    .join(df_marco_agg.withColumnRenamed("ID_TRAMITE", "CODIGO_TRAMITE"), "CODIGO_TRAMITE", "left") \
    .join(df_turno_agg.withColumnRenamed("ID_TRAMITE", "CODIGO_TRAMITE"), "CODIGO_TRAMITE", "left") \
    .withColumn("FECHA_ALTA_TRAMITE", F.to_timestamp(F.from_unixtime(F.col("created").cast("long")))) \
    .withColumn("FECHA_ULT_MODIFICACION_TRAMITE", F.to_timestamp(F.from_unixtime(F.col("changed").cast("long")))) \
    .na.fill({"FINALIDAD": "Sin finalidad", "ID_DEPENDENCIA": "0"})

df_final = df_flat.select(
    "CODIGO_TRAMITE", "FECHA_ALTA_TRAMITE", "FECHA_ULT_MODIFICACION_TRAMITE", "NOMBRE", "FINALIDAD", "ID_DEPENDENCIA", "ID_TIPO_TRAMITE",
    "NIVEL_MIBA", "MONTO_COSTO", "CONCEPTO_PAGO", "REQUIERE_PAGO", "MARCO_NORMATIVO", "CANAL_TURNO",
    F.lit(None).cast("timestamp").alias("FECHA_BAJA_TRAMITE"), # Fix Missing Fields
    F.lit("No especificado").alias("TIEMPO_RESPUESTA_ESTIMADO"),
    F.lit(None).cast(StringType()).alias("CONCLUSION_TRAMITACION_DOC")
).withColumn("ID_TRAMITE", F.col("CODIGO_TRAMITE"))

save_table(df_final, dbName, dbName)

job.commit()