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
          .withColumn("proficiencia", F.round(F.col("proficiencia").cast(DoubleType()), 2))
          .withColumn("peso_aluno", F.round(F.col("peso_aluno").cast(DoubleType()), 2))
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
        .withColumn("taxa_alfabetizacao", F.round(F.col("taxa_alfabetizacao").cast(DoubleType()), 2))
        .withColumn("percentual_participacao", F.round(F.col("percentual_participacao").cast(DoubleType()), 2))
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
         .withColumn("taxa_alfabetizacao", F.round(F.col("taxa_alfabetizacao").cast(DoubleType()), 2))
         .withColumn("media_portugues", F.round(F.col("media_portugues").cast(DoubleType()), 2))
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

def transformar_df_bronze(entidade):
  path_input = f"{BASE_BRONZE}{entidade}/_ing_ano={ano}/_ing_mes={mes}/"

  try:
      log.info(f"[PROC:SILVER] Lendo Bronze de: {path_input}")
      df_bronze = spark.read.parquet(path_input)
      log.info(f"[PROC:SILVER] {df_bronze.count()} registros lidos da Bronze")
      if entidade == "avaliacao_aluno":
        df_bronze = transformar_aluno(df_bronze)
      elif entidade == "meta_brasil" or entidade == "meta_municipio" or entidade == "meta_uf":
        df_bronze = transformar_meta(df_bronze, entidade)
      elif entidade == "municipio" or entidade == "uf":
        df_bronze = transformar_regional(df_bronze, entidade)
      elif entidade == "mapeamento_municipio":
        df_bronze = transformar_mapeamento_m(df_bronze)
      tratamento_duplicidade(df_bronze, entidade)
  except Exception as e:
          log.info(f"[PROC:SILVER] Erro ao processar {entidade}: {str(e)}")
          raise e

  return df_bronze

def constroi_fato_aluno(dataframes):

  # Tabela fato com informações dos alunos.
  fct_avaliacao_aluno = dataframes["avaliacao_aluno"] \
                        .join(dataframes["mapeamento_municipio"], on="id_municipio", how="left")\
                        .join(dataframes["municipio"], on=["ano", "id_municipio", "rede"], how="left") \
                        .join(dataframes["uf"], on=["ano", "sigla_uf", "rede"], how="left") \
                        .select("id_aluno",
                                "ano",
                                "rede",
                                "proficiencia",
                                "peso_aluno",
                                "compareceu",
                                "preencheu_caderno",
                                "alfabetizado",
                                "nivel_alfabetizacao",
                                "id_escola",
                                "nome_municipio",
                                "sigla_uf",
                                dataframes["municipio"].media_portugues.alias("media_portugues_municipio"),
                                dataframes["uf"].media_portugues.alias("media_portugues_estado")
                                ) \
                        .fillna("Não encontrado", subset=["nome_municipio", "sigla_uf"]) \
                        .fillna(0.0, subset=["media_portugues_municipio", "media_portugues_estado"])

  return fct_avaliacao_aluno \
          .withColumn("_silver_processed_at", F.lit(datetime.now(timezone.utc).isoformat())) \
          .withColumn("_proc_ano", F.lit(ano)) \
          .withColumn("_proc_mes", F.lit(mes))

def constroi_fato_municipio(dataframes):
  # Construção da dimensão de município.
  dim_municipio = dataframes["municipio"] \
                  .join(dataframes["mapeamento_municipio"], on="id_municipio", how="left")\
                  .select("ano",
                          "id_municipio",
                          "nome_municipio",
                          "sigla_uf",
                          "serie",
                          "rede",
                          "taxa_alfabetizacao",
                          "media_portugues",
                          *[col for col in dataframes["municipio"].columns if col.startswith("pc")]
                          ) \
                  .fillna("Não encontrado", subset=["nome_municipio", "sigla_uf"])

  # Tabela fato com informações municipais.
  fct_meta_municipio = dataframes["meta_municipio"] \
                      .join(dim_municipio, on=["ano", "id_municipio"], how="inner") \
                      .join(dataframes["meta_uf"], on=["ano", "sigla_uf"], how="inner") \
                      .select("nome_municipio",
                              "sigla_uf",
                              "ano",
                              dim_municipio.rede,
                              dim_municipio.taxa_alfabetizacao,
                              dataframes["meta_municipio"].nivel_alfabetizacao.alias("nivel_alfabetizacao_municipal"),
                              dataframes["meta_municipio"].taxa_alfabetizacao.alias("taxa_alfabetizacao_municipal"),
                              dataframes["meta_municipio"].percentual_participacao.alias("percentual_participacao_municipal"),
                              dataframes["meta_uf"].taxa_alfabetizacao.alias("taxa_alfabetizacao_estadual"),
                              dataframes["meta_uf"].percentual_participacao.alias("percentual_participacao_estadual"),
                              dataframes["meta_municipio"].meta_2024,
                              dataframes["meta_municipio"].meta_2025,
                              dataframes["meta_municipio"].meta_2026,
                              dataframes["meta_municipio"].meta_2027,
                              dataframes["meta_municipio"].meta_2028,
                              dataframes["meta_municipio"].meta_2029,
                              dataframes["meta_municipio"].meta_2030
                              )

  return fct_meta_municipio \
          .withColumn("_silver_processed_at", F.lit(datetime.now(timezone.utc).isoformat())) \
          .withColumn("_proc_ano", F.lit(ano)) \
          .withColumn("_proc_mes", F.lit(mes))

def constroi_fato_estado(dataframes):
  # Tabela fato com informações estaduais.
  fct_meta_uf = dataframes["meta_uf"] \
                      .join(dataframes["uf"], on=["ano", "sigla_uf"], how="inner") \
                      .join(dataframes["meta_brasil"], on=["ano"], how="inner") \
                      .select("sigla_uf",
                              "ano",
                              dataframes["uf"].rede,
                              dataframes["uf"].taxa_alfabetizacao,
                              dataframes["meta_uf"].taxa_alfabetizacao.alias("taxa_alfabetizacao_estadual"),
                              dataframes["meta_uf"].percentual_participacao.alias("percentual_participacao_estadual"),
                              dataframes["meta_brasil"].taxa_alfabetizacao.alias("taxa_alfabetizacao_nacional"),
                              dataframes["meta_brasil"].percentual_participacao.alias("percentual_participacao_nacional"),
                              dataframes["meta_uf"].meta_2024,
                              dataframes["meta_uf"].meta_2025,
                              dataframes["meta_uf"].meta_2026,
                              dataframes["meta_uf"].meta_2027,
                              dataframes["meta_uf"].meta_2028,
                              dataframes["meta_uf"].meta_2029,
                              dataframes["meta_uf"].meta_2030
                      )

  return fct_meta_uf \
          .withColumn("_silver_processed_at", F.lit(datetime.now(timezone.utc).isoformat())) \
          .withColumn("_proc_ano", F.lit(ano)) \
          .withColumn("_proc_mes", F.lit(mes))

def salvar_camada_silver(df, path_output):
  log.info(f"[SILVER] Salvando em: {path_output}")
  df.write.format("parquet").mode("overwrite").partitionBy("_proc_ano", "_proc_mes").save(path_output)
  log.info(f"[SILVER] {df.count()} registros salvos.")
  return path_output

def checar_qualidade_silver(fact_table, fact_table_name):
  dq_checks = DQ_CHECKS[fact_table_name]
  log.info("=" * 60)
  log.info(f"[DQ:SILVER] Iniciando verificacoes {fact_table_name.upper()} | checks = {len(dq_checks)}")

  for check in dq_checks:
    tipo    = check["tipo"]
    coluna  = check.get("coluna")
    valor   = check.get("valor")
    mensagem_log = ""
    ok      = False

    try:
      if tipo == "not_null":
          nulos   = fact_table.filter(F.col(coluna).isNull()).count()
          ok      = (nulos == 0)
          mensagem_log = f"{nulos} nulos encontrados"
      elif tipo == "min_count":
          contagem = fact_table.count()
          ok       = (contagem >= valor)
          mensagem_log  = f"contagem = {contagem} | minimo = {valor}"
      elif tipo == "unique":
          dups    = fact_table.count() - fact_table.select(coluna).distinct().count()
          ok      = (dups == 0)
          mensagem_log = f"{dups} duplicatas encontradas"
      elif tipo == "range":
          mn, mx = valor
          fora   = fact_table.filter((F.col(coluna) < mn) | (F.col(coluna) > mx)).count()
          ok      = (fora == 0)
          mensagem_log = f"{fora} fora do intervalo [{mn},{mx}]"
    except Exception as e:
      ok      = False
      mensagem_log = f"Erro: {e}"

    if ok:
      log.info(f"[DQ:SILVER] PASS | {tipo} | coluna = {coluna} | {mensagem_log}")
    else:
      log.error(f"[DQ:SILVER] FAIL | {tipo} | coluna = {coluna} | {mensagem_log}")
      raise Exception(f"[DQ:SILVER] Check {coluna}:{tipo} falhou. Job interrompido.")

DQ_CHECKS = {
    "fct_avaliacao_aluno": [
        {"tipo": "min_count", "valor": 1},
        {"tipo": "not_null",  "coluna": "id_aluno"},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "id_escola"},
        {"tipo": "range",     "coluna": "proficiencia", "valor": (0, 1000)},
        {"tipo": "range",     "coluna": "nivel_alfabetizacao", "valor": (0, 8)},
        {"tipo": "range",     "coluna": "media_portugues_municipio", "valor": (0, 1000)},
        {"tipo": "range",     "coluna": "media_portugues_estado", "valor": (0, 1000)},
    ],
    "fct_meta_municipio": [
        {"tipo": "min_count", "valor": 1,},
        {"tipo": "not_null",  "coluna": "nome_municipio"},
        {"tipo": "not_null",  "coluna": "sigla_uf"},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "nivel_alfabetizacao_municipal", "valor": (0, 8)},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao_municipal", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "percentual_participacao_municipal", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao_estadual", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "percentual_participacao_estadual", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2024", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2025", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2026", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2027", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2028", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2029", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2030", "valor": (0, 100)},
    ],
    "fct_meta_uf": [
        {"tipo": "min_count", "valor": 1},
        {"tipo": "not_null",  "coluna": "sigla_uf"},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao_estadual", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "percentual_participacao_estadual", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao_nacional", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "percentual_participacao_nacional", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2024", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2025", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2026", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2027", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2028", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2029", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "meta_2030", "valor": (0, 100)},
    ],
}

dataframes = {
    "avaliacao_aluno": None,
    "meta_brasil": None,
    "meta_municipio": None,
    "meta_uf": None,
    "municipio": None,
    "uf": None,
    "mapeamento_municipio": None
}

facts = {
    "fct_avaliacao_aluno": None,
    "fct_meta_municipio": None,
    "fct_meta_uf": None
}

log.info("=" * 60)
log.info("INICIANDO PROCESSAMENTO SILVER")
log.info(f"  Lendo de  : {BASE_BRONZE}")
log.info(f"  Destino  : {BASE_SILVER}")

try:
  # Loop de processamento e transformação dos arquivos bronze.
  for entidade in dataframes.keys():
      log.info("=" * 60)
      log.info(f"[PROC:SILVER] Processando Entidade: {entidade.upper()}")

      dataframes[entidade] = transformar_df_bronze(entidade)

  facts["fct_avaliacao_aluno"], facts["fct_meta_municipio"], facts["fct_meta_uf"] = \
   [constroi_fato_aluno(dataframes), constroi_fato_municipio(dataframes), constroi_fato_estado(dataframes)]

  for fact_name, fact_table in facts.items():
    path_output = f"{BASE_SILVER}{fact_name}/"
    checar_qualidade_silver(fact_table, fact_name)
    silver_path = salvar_camada_silver(fact_table, path_output)

    log.info("=" * 60)
    log.info("SUMARIO SILVER")
    log.info(f"  Lido de  : {BASE_BRONZE}")
    log.info(f"  Destino  : {silver_path}_proc_ano={ano}/_proc_mes={mes}/")
    log.info(f"  Pipeline completo para entidade: {fact_name.upper()}")

except Exception as e:
  log.info(f"[PROC:SILVER] Erro ao processar a camada SILVER: {str(e)}")

log.info("=" * 60)
log.info("[PROC:SILVER] Processamento da Camada Silver Finalizado!")
