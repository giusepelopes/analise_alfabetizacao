import sys
import logging
from datetime import datetime, timezone

from google.colab import drive
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, BooleanType

drive.mount('/content/drive')

# Configurando LOGS para serem registrados durante a execução do job.
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ", force=True)
log = logging.getLogger(__name__)

# Configurando Spark Session
spark = SparkSession.builder.appName("Pipeline-Silver").getOrCreate()

# Variáveis
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ano, mes, dia  = INGESTION_DATE.split("-")

BASE_BRONZE = "/content/drive/MyDrive/projeto-medalhao/bronze/"
BASE_SILVER = "/content/drive/MyDrive/projeto-medalhao/silver/"

# Definicao de chaves primarias de cada entidade, para verificacao de duplicidade.
P_KEYS = {
    "avaliacao_aluno": ["ano", "id_aluno"],
    "meta_brasil": ["ano"],
    "meta_municipio": ["ano", "id_municipio"],
    "meta_uf": ["ano", "sigla_uf"],
    "municipio": ["ano", "id_municipio", "rede"],
    "uf": ["ano", "sigla_uf", "rede"],
    "mapeamento_municipio": ["id_municipio"]
}

def tratamento_duplicidade(df, entidade):
  log.info(f"[SILVER] Verificando duplicidade: {entidade.upper()}")

  chaves = P_KEYS[entidade]

  total_linhas = df.count()
  linhas_unicas = df.select(chaves).distinct().count()

  if total_linhas == linhas_unicas:
      log.info(f"[SILVER] {entidade.upper()}: OK (Nenhuma duplicidade encontrada nas chaves {chaves}). Total: {total_linhas} linhas.")
  else:
      duplicados = total_linhas - linhas_unicas
      log.info(f"[SILVER] {entidade.upper()}: ATENÇÃO! Encontradas {duplicados} linhas duplicadas com base nas chaves {chaves}.")
      # Remove duplicados mantendo o primeiro registro encontrado para aquela chave
      df = df.dropDuplicates(chaves)
      log.info(f"[SILVER] Deduplicacao: {total_linhas - df.count()} removidos (chave={chaves})")

  return df

# Renomeação de colunas, preenchimento de valores nulos e casting para tipos esperados.
def transformar_aluno(df):
  log.info("[SILVER] Aplicando regras: AVALIACAO_ALUNO")

  return (df
          .withColumnRenamed("presenca", "compareceu")
          .withColumnRenamed("preenchimento_caderno", "preencheu_caderno")
          .fillna(0, subset=["proficiencia", "peso_aluno"])
          .withColumn("ano", F.col("ano").cast(IntegerType()))
          .withColumn("id_municipio", F.col("id_municipio").cast(IntegerType()))
          .withColumn("id_escola", F.col("id_escola").cast(IntegerType()))
          .withColumn("id_aluno", F.col("id_aluno").cast(IntegerType()))
          .withColumn("caderno", F.col("caderno").cast(IntegerType()))
          .withColumn("serie", F.col("serie").cast(IntegerType()))
          .withColumn("rede",   F.when(F.col("rede") == 1, "Federal")
                                .when(F.col("rede") == 2, "Estadual")
                                .when(F.col("rede") == 3, "Municipal")
                                .otherwise("Privada"))
          .withColumn("compareceu", F.col("compareceu").cast(BooleanType()))
          .withColumn("preencheu_caderno", F.col("preencheu_caderno").cast(BooleanType()))
          .withColumn("alfabetizado", F.col("alfabetizado").cast(BooleanType()))
          .withColumn("proficiencia", F.col("proficiencia").cast(DoubleType()))
          .withColumn("peso_aluno", F.col("peso_aluno").cast(DoubleType()))
          .withColumn("nivel_alfabetizacao", F
                     .when(F.col("proficiencia") < 650, 0)
                     .when(F.col("proficiencia") < 675, 1)
                     .when(F.col("proficiencia") < 700, 2)
                     .when(F.col("proficiencia") < 725, 3)
                     .when(F.col("proficiencia") < 750, 4)
                     .when(F.col("proficiencia") < 775, 5)
                     .when(F.col("proficiencia") < 800, 6)
                     .when(F.col("proficiencia") < 825, 7)
                     .otherwise(8))
          )

def transformar_meta(df, entidade):
  log.info(f"[SILVER] Aplicando regras: {entidade.upper()}")
  colunas = {
        "meta_alfabetizacao_2024":    "meta_2024",
        "meta_alfabetizacao_2025":    "meta_2025",
        "meta_alfabetizacao_2026":    "meta_2026",
        "meta_alfabetizacao_2027":    "meta_2027",
        "meta_alfabetizacao_2028":    "meta_2028",
        "meta_alfabetizacao_2029":    "meta_2029",
        "meta_alfabetizacao_2030":    "meta_2030",
    }

  for original, novo in colunas.items():
    if original in df.columns:
      df = df.withColumnRenamed(original, novo).fillna({novo: 0}).withColumn(novo, F.col(novo).cast(DoubleType()))

  df = (df
        .withColumn("ano", F.col("ano").cast(IntegerType()))
        .withColumn("rede", F.initcap(F.trim(F.col("rede"))))
        .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast(DoubleType()))
        .withColumn("percentual_participacao", F.col("percentual_participacao").cast(DoubleType()))
        )

  if entidade == "meta_municipio":
    df = df.fillna(0, subset=["taxa_alfabetizacao", "nivel_alfabetizacao", "percentual_participacao"])
    df = (df
         .withColumn("id_municipio", F.col("id_municipio").cast(IntegerType()))
         .withColumn("nivel_alfabetizacao", F.col("nivel_alfabetizacao").cast(IntegerType()))
         )
  elif entidade == "meta_uf":
    df = df.fillna(0, subset=["taxa_alfabetizacao", "percentual_participacao"])
    df = df.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))

  return df

def transformar_regional(df, entidade):
  log.info(f"[SILVER] Aplicando regras: {entidade.upper()}")
  colunas = {
        "proporcao_aluno_nivel_0":    "pc_nivel_0",
        "proporcao_aluno_nivel_1":    "pc_nivel_1",
        "proporcao_aluno_nivel_2":    "pc_nivel_2",
        "proporcao_aluno_nivel_3":    "pc_nivel_3",
        "proporcao_aluno_nivel_4":    "pc_nivel_4",
        "proporcao_aluno_nivel_5":    "pc_nivel_5",
        "proporcao_aluno_nivel_6":    "pc_nivel_6",
        "proporcao_aluno_nivel_7":    "pc_nivel_7",
        "proporcao_aluno_nivel_8":    "pc_nivel_8",
    }

  df = df.fillna(0, subset=["taxa_alfabetizacao", "media_portugues"])
  for original, novo in colunas.items():
    if original in df.columns:
      df = df.withColumnRenamed(original, novo).fillna({novo: 0}).withColumn(novo, F.col(novo).cast(DoubleType()))

  df = (df
         .withColumn("ano", F.col("ano").cast(IntegerType()))
         .withColumn("serie", F.col("serie").cast(IntegerType()))
         .withColumn("rede", F.initcap(F.trim(F.col("rede"))))
         .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast(DoubleType()))
         .withColumn("media_portugues", F.col("media_portugues").cast(DoubleType()))
         .withColumn("rede",   F.when(F.col("rede") == 0, "Total")
                                .when(F.col("rede") == 1, "Federal")
                                .when(F.col("rede") == 2, "Estadual")
                                .when(F.col("rede") == 3, "Municipal")
                                .when(F.col("rede") == 4, "Privada")
                                .when(F.col("rede") == 5, "Publica (E,M)")
                                .otherwise("Publica (F,E,M)"))
         )

  if entidade == "municipio":
    df = df.withColumn("id_municipio", F.col("id_municipio").cast(IntegerType()))
  elif entidade == "uf":
    df = df.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))

  return df

def transformar_mapeamento_m(df):
  log.info("[SILVER] Aplicando regras: MAPEAMENTO_MUNICIPIO")
  return (df
          .withColumn("id_municipio", F.col("id_municipio").cast(IntegerType()))
          .withColumn("nome_municipio", F.trim(F.col("nome_municipio")))
          .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
          )

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

# Loop de processamento dos arquivos bronze.
for origem, destino in entidades.items():
    log.info("=" * 60)
    log.info(f"[PROC:SILVER] Processando Entidade: {origem.upper()}")

    path_input = f"{BASE_BRONZE}{origem}/_ing_ano={ano}/_ing_mes={mes}/"
    path_output = f"{BASE_SILVER}{destino}/"

    try:
        log.info(f"[PROC:SILVER] Lendo Bronze de: {path_input}")
        df_bronze = spark.read.parquet(path_input)
        log.info(f"[PROC:SILVER] {df_bronze.count()} registros lidos da Bronze")
        if origem == "avaliacao_aluno":
          df_bronze = transformar_aluno(df_bronze)
        elif origem == "meta_brasil" or origem == "meta_municipio" or origem == "meta_uf":
          df_bronze = transformar_meta(df_bronze, origem)
        elif origem == "municipio" or origem == "uf":
          df_bronze = transformar_regional(df_bronze, origem)
        elif origem == "mapeamento_municipio":
          df_bronze = transformar_mapeamento_m(df_bronze)
        df_bronze = tratamento_duplicidade(df_bronze, origem)
        df_bronze.show(5, truncate=False)
    except Exception as e:
            log.info(f"[PROC:SILVER] Erro ao processar {origem}: {str(e)}")

