import sys
import logging
import hashlib
from datetime import datetime, timezone

from google.colab import drive
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

drive.mount('/content/drive')

# Configurando LOGS para serem registrados durante a execução do job.
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ", force=True)
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
      log.info(f"[INGESTAO] Falha ao ingerir arquivos em {path}: {str(e)}")
      raise

def construir_bronze(df_raw, entity):
  log.info(f"[BRONZE] Adicionando metadados.")

  # Para construir o hash, captura as colunas originais lidas antes de adicionar os metadados.
  original_columns = df_raw.columns

  # Adiciona metadados essenciais
  df_bronze = df_raw \
      .withColumn("_ingestion_timestamp", F.lit(INGESTION_TS)) \
      .withColumn("_ingestion_date", F.lit(INGESTION_DATE)) \
      .withColumn("_source_file", F.input_file_name()) \
      .withColumn("_source_entity", F.lit(entity)) \
      .withColumn("_record_hash", F.sha2(F.concat_ws("||", *original_columns), 256)) \
      .withColumn("_ing_ano", F.lit(ano)) \
      .withColumn("_ing_mes", F.lit(mes))

  return df_bronze

def salvar_camada_bronze(df_bronze, path_output):
  log.info(f"[BRONZE] Salvando em: {path_output}")
  df_bronze.write.format("parquet").mode("overwrite").partitionBy("_ing_ano", "_ing_mes").save(path_output)
  log.info(f"[BRONZE] {df_bronze.count()} registros salvos.")
  return path_output

def checar_qualidade_bronze(df_bronze, dq_checks):
  log.info(f"[DQ:BRONZE] Iniciando verificacoes | checks = {len(dq_checks)}")

  for check in dq_checks:
    tipo    = check["tipo"]
    coluna  = check.get("coluna")
    valor   = check.get("valor")
    mensagem_log = ""
    ok      = False

    try:
      if tipo == "not_null":
          nulos   = df_bronze.filter(F.col(coluna).isNull()).count()
          ok      = nulos == 0
          mensagem_log = f"{nulos} nulos encontrados"
      elif tipo == "min_count":
          contagem = df_bronze.count()
          ok       = contagem >= valor
          mensagem_log  = f"contagem = {contagem} | minimo = {valor}"
      elif tipo == "unique":
          dups    = df_bronze.count() - df_bronze.select(coluna).distinct().count()
          ok      = dups == 0
          mensagem_log = f"{dups} duplicatas encontradas"
    except Exception as e:
      ok      = False
      mensagem_log = f"Erro: {e}"

    if ok:
      log.info(f"[DQ:BRONZE] PASS | {tipo} | coluna = {coluna} | {mensagem_log}")
    else:
      log.error(f"[DQ:BRONZE] FAIL | {tipo} | coluna = {coluna} | {mensagem_log}")
      raise Exception(f"[DQ:BRONZE] Check {coluna}:{tipo} falhou. Job interrompido.")

# Verificações de qualidade de dados, a serem feitas antes do salvamento da camada bronze.
DQ_CHECKS = {
    "avaliacao_aluno": [
        {"tipo": "min_count", "valor":  1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "id_municipio"},
        {"tipo": "not_null",  "coluna": "id_escola"},
        {"tipo": "not_null",  "coluna": "id_aluno"},
    ],
    "meta_brasil": [
        {"tipo": "min_count", "valor":  1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "unique",    "coluna": "ano"},
    ],
    "meta_municipio": [
        {"tipo": "min_count", "valor":  1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "id_municipio"},
    ],
    "meta_uf": [
        {"tipo": "min_count", "valor":  1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "sigla_uf"},
    ],
    "municipio": [
        {"tipo": "min_count", "valor":  1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "id_municipio"},
    ],
    "uf": [
        {"tipo": "min_count", "valor":  1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "sigla_uf"},
    ],
    "mapeamento_municipio": [
        {"tipo": "min_count", "valor":  1},
        {"tipo": "not_null",  "coluna": "id_municipio"},
        {"tipo": "not_null",  "coluna": "nome_municipio"},
        {"tipo": "not_null",  "coluna": "sigla_uf"},
    ],
}

# Mapeamento de entidades, seguindo o formato (Pasta de Origem -> Pasta de Destino).
entidades = {
    "avaliacao_aluno": "avaliacao_aluno",
    "meta_brasil": "meta_brasil",
    "meta_municipio": "meta_municipio",
    "meta_uf": "meta_uf",
    "municipio": "municipio",
    "uf": "uf",
    "mapeamento_municipio": "mapeamento_municipio"
}

log.info("=" * 60)
log.info("INICIANDO PROCESSAMENTO BRONZE")
log.info(f"  Lendo de  : {BASE_LANDING}")
log.info(f"  Destino  : {BASE_BRONZE}")

# Loop de processamento dos arquivos raw.
for origem, destino in entidades.items():
    log.info("=" * 60)
    log.info(f"[PROC:BRONZE] Processando Entidade: {origem.upper()}")

    path_input = f"{BASE_LANDING}{origem}/"
    path_output = f"{BASE_BRONZE}{destino}/"

    try:
        df_raw = ingerir_dados(path_input)
        df_bronze = construir_bronze(df_raw, origem)

        dq_checks = DQ_CHECKS.get(origem, [])
        if dq_checks:
          checar_qualidade_bronze(df_bronze, dq_checks)
        else:
          log.warning(f"[DQ:BRONZE] Nenhuma regra definida para '{origem}' — pulando verificacao")

        bronze_path = salvar_camada_bronze(df_bronze, path_output)

        log.info("=" * 60)
        log.info("SUMARIO BRONZE")
        log.info(f"  Lido de  : {BASE_LANDING}")
        log.info(f"  Destino  : {bronze_path}_ing_ano={ano}/_ing_mes={mes}/")
        log.info(f"  Pipeline completo para entidade: {origem.upper()}")

    except Exception as e:
        log.info(f"[PROC:BRONZE] Erro ao processar {origem}: {str(e)}")

log.info("=" * 60)
log.info("[PROC:BRONZE] Ingestão da Camada Bronze Finalizado!")
