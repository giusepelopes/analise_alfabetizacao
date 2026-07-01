import sys
import logging
import hashlib
from datetime import datetime, timezone

from google.colab import drive
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

drive.mount('/content/drive')

# Configurando LOGS para serem registrados durante a execução do job.
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
log = logging.getLogger(__name__)

# Configurando Spark Session
spark = SparkSession.builder.appName("Pipeline-Bronze").getOrCreate()

# Váriaveis
INGESTION_TS   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ano, mes, dia  = INGESTION_DATE.split("-")

# Caminhos Base
BASE_LANDING = "/content/drive/MyDrive/projeto-medalhao/landing_zone/"
BASE_BRONZE = "/content/drive/MyDrive/projeto-medalhao/bronze/"

def ingerir_dados(path):
  log.info(f"[INGESTAO] Ingerindo arquivos: {path}")
  try:
      df_raw = spark.read.format("csv") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .option("delimiter", ",") \
          .load(path)

      log.info(f"[INGESTAO] {df_raw.count()} registros ingeridos.")
      return df_raw
  except Exception as e:
      print(f"[INGESTAO] Falha ao ingerir arquivos em {path}: {str(e)}")
      raise

def construir_bronze(df_raw, entity):
  log.info(f"[BRONZE] Adicionando metadados")

  # Para construir o hash, captura as colunas originais lidas antes de adicionar os metadados.
  original_columns = df_raw.columns

  # Adiciona metadados essenciais
  df_bronze = df_raw \
      .withColumn("_ingestion_timestamp", F.lit(INGESTION_TS)) \
      .withColumn("_ingestion_date", F.lit(INGESTION_DATE)) \
      .withColumn("_source_file", F.input_file_name()) \
      .withColumn("_source_entity", F.lit(entity)) \
      .withColumn("_record_hash", F.sha2(F.concat_ws("||", *original_columns), 256)) \
      .withColumn("ing_ano", F.lit(ano)) \
      .withColumn("ing_mes", F.lit(mes)) \
      .withColumn("ing_dia", F.lit(dia))

  return df_bronze

# Mapeamento de entidades, seguindo o formato (Pasta de Origem -> Pasta de Destino).
entidades = {
    "avaliacao_aluno": "avaliacao_aluno",
    "meta_brasil": "meta_brasil",
    "meta_municipio": "meta_municipio",
    "meta_uf": "meta_uf",
    "municipio": "municipio",
    "uf": "uf"
}

# Loop de processamento dos arquivos raw.
for origem, destino in entidades.items():
    log.info(f"[PROC:BRONZE] Processando Entidade: {destino.upper()}")

    path_input = f"{BASE_LANDING}{origem}/"
    path_output = f"{BASE_BRONZE}{destino}/"

    try:
        df_raw = ingerir_dados(path_input)
        df_bronze = construir_bronze(df_raw, origem)
        df_bronze.show(5, truncate=False)

    except Exception as e:
        print(f"Erro ao processar {destino}: {str(e)}")

print("\n Pipeline de Ingestão da Camada Bronze Finalizado!")
