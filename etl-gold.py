import sys
import logging
from datetime import datetime, timezone

from google.colab import drive
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, BooleanType

drive.mount('/content/drive')

# Configurando LOGS
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ", force=True)
log = logging.getLogger(__name__)

# Configurando Spark Session
spark = SparkSession.builder.appName("Pipeline-Gold").getOrCreate()

# Variáveis
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ano, mes, dia  = INGESTION_DATE.split("-")

BASE_SILVER = "/content/drive/MyDrive/projeto-medalhao/silver/"
BASE_GOLD   = "/content/drive/MyDrive/projeto-medalhao/gold/"

# Tabela gold com uma visão consolidada da alfabetização municipal, para uso em dashboards.
# Utiliza a tabela fato fct_meta_municipio.
def gold_consolidado_municipio(fact_tables):
  df_silver = fact_tables["fct_meta_municipio"]

  # Obtendo a meta do ano corrente, para em seguida conseguir verificar se a meta foi atingida.
  df_silver = df_silver.withColumn("meta_ano_corrente",
                                    F.expr("""
                                    CASE
                                    WHEN ano = 2024 THEN meta_2024
                                    WHEN ano = 2025 THEN meta_2025
                                    WHEN ano = 2026 THEN meta_2026
                                    WHEN ano = 2027 THEN meta_2027
                                    WHEN ano = 2028 THEN meta_2028
                                    WHEN ano = 2029 THEN meta_2029
                                    WHEN ano = 2030 THEN meta_2030
                                    ELSE 0.0
                                    END
                                    """))

  return (df_silver
          .groupBy("ano", "sigla_uf", "nome_municipio", "rede")
          .agg(
              F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_real"),
              F.round(F.avg("meta_ano_corrente"), 2).alias("meta_alfabetizacao_esperada"),
              F.round(F.avg("percentual_participacao_municipal"), 2).alias("percentual_participacao"),
              F.round(F.avg("taxa_alfabetizacao_estadual"), 2).alias("taxa_alfabetizacao_referencia_estado")
          )
          .withColumn("atingiu_meta",
                      F.when(F.col("taxa_alfabetizacao_real") >= F.col("meta_alfabetizacao_esperada"), True)
                      .otherwise(False)
                      )
          .withColumn("diff_meta", F.col("taxa_alfabetizacao_real") - F.col("meta_alfabetizacao_esperada"))
          .withColumn("_gold_processed_at", F.lit(datetime.now(timezone.utc).isoformat())))

# Tabela gold com uma visão consolidada da alfabetização estadual, fazendo uso da tabela fato fct_meta_uf.
def gold_consolidado_uf(fact_tables):
  df_silver = fact_tables["fct_meta_uf"]

  df_silver = df_silver.withColumn("meta_ano_corrente",
      F.expr("""
      CASE
      WHEN ano = 2024 THEN meta_2024
      WHEN ano = 2025 THEN meta_2025
      WHEN ano = 2026 THEN meta_2026
      WHEN ano = 2027 THEN meta_2027
      WHEN ano = 2028 THEN meta_2028
      WHEN ano = 2029 THEN meta_2029
      WHEN ano = 2030 THEN meta_2030
      ELSE 0.0
      END
      """))

  return (df_silver
          .groupBy("ano", "sigla_uf", "rede")
          .agg(
              F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_real_uf"),
              F.round(F.avg("meta_ano_corrente"), 2).alias("meta_alfabetizacao_esperada_uf"),
              F.round(F.avg("percentual_participacao_estadual"), 2).alias("percentual_participacao_uf"),
              F.round(F.avg("taxa_alfabetizacao_nacional"), 2).alias("taxa_alfabetizacao_referencia_brasil")
          )
          .withColumn("uf_atingiu_meta",
              F.when(F.col("taxa_alfabetizacao_real_uf") >= F.col("meta_alfabetizacao_esperada_uf"), True)
              .otherwise(False)
          )
          .withColumn("diff_meta", F.col("taxa_alfabetizacao_real_uf") - F.col("meta_alfabetizacao_esperada_uf"))
          .withColumn("_gold_processed_at", F.lit(datetime.now(timezone.utc).isoformat())))

# Tabela gold com a distribuição anual por nível de alfabetização, para cada tipo de rede de ensino.
# Construída a partir da tabela fct_avaliacao_aluno.
def gold_distribuicao_niveis(fact_tables):
  df_silver = fact_tables["fct_avaliacao_aluno"]

  # Filtrando apenas alunos que compareceram e preencheram o caderno, para consistência,
  # e contando quantos alunos foram avaliados, agrupados pelas colunas de interesse.
  df_gold = (df_silver.filter((F.col("compareceu") == True) & (F.col("preencheu_caderno") == True))
                  .groupBy("ano", "sigla_uf", "nome_municipio", "rede", "nivel_alfabetizacao")
                  .agg(F.count("id_aluno").alias("quantidade_alunos")))

  # Usando função de janela para cálculo do percentual, para evitar joins adicionais.
  window = Window.partitionBy("ano", "sigla_uf", "nome_municipio", "rede")

  df_gold = (df_gold
              .withColumn("total_alunos_contexto", F.sum("quantidade_alunos").over(window))
              .withColumn("percentual_alunos", F.round((F.col("quantidade_alunos") / F.col("total_alunos_contexto")) * 100, 2))
              .drop("total_alunos_contexto")
              .withColumn("_gold_processed_at", F.lit(datetime.now(timezone.utc).isoformat())))

  return df_gold

# Tabela com estatisticas de desempenho das escolas que participaram da avaliação, construída a partir da tabela fct_avaliacao_aluno.
def gold_estatisticas_escolares(fact_tables):
    df_silver = fact_tables["fct_avaliacao_aluno"]

    return (df_silver
        .groupBy("ano", "sigla_uf", "nome_municipio", "id_escola", "rede")
        .agg(
            F.count("id_aluno").alias("total_alunos"),
            F.sum(F.when(F.col("compareceu") == True, 1).otherwise(0)).alias("total_alunos_presentes"),
            F.round(
                (F.sum(F.when(F.col("compareceu") == False, 1).otherwise(0)) / F.count("id_aluno")) * 100, 2
            ).alias("percentual_faltantes"),
            F.round(F.mean(F.when(F.col("compareceu") == True, F.col("proficiencia"))), 2).alias("media_proficiencia"),
            F.round(F.stddev(F.when(F.col("compareceu") == True, F.col("proficiencia"))), 2).alias("desvio_padrao_proficiencia"),
            F.when(
                F.sum(F.when(F.col("compareceu") == True, 1).otherwise(0)) > 0,
                F.round(
                  (F.sum(F.when((F.col("compareceu") == True) & (F.col("alfabetizado") == True), 1).otherwise(0)) /
                  F.sum(F.when(F.col("compareceu") == True, 1).otherwise(0))) * 100, 2
                )
            ).otherwise(0).alias("taxa_alfabetizacao_escola"))
        # Preenche com 0 caso alguma escola não tenha tido nenhum aluno presente
        .fillna(0, subset=["desvio_padrao_proficiencia", "media_proficiencia", "taxa_alfabetizacao_escola"])
        .withColumn("_gold_processed_at", F.current_timestamp()))

# Tabela construída para uso em treinamento de modelos de machine learning,
# tanto para modelos de classificação binária (alfabetizado ou não), quanto para regressão (nível de alfabetização).
def gold_ml_aluno(fact_tables):
  df_silver = fact_tables["fct_avaliacao_aluno"]

  df_silver = df_silver.filter((F.col("compareceu") == True) & (F.col("preencheu_caderno") == True))

  window_escola = Window.partitionBy("ano", "id_escola")

  return (df_silver
        .withColumn("feat_media_proficiencia_escola", F.round(F.mean("proficiencia").over(window_escola), 2))
        .select(
            "ano",
            "id_aluno",
            F.when(F.col("rede") == "Municipal", 1)
             .when(F.col("rede") == "Estadual", 2)
             .when(F.col("rede") == "Federal", 3)
             .otherwise(4).alias("feat_rede_encoded"),
            F.col("peso_aluno").alias("feat_peso_aluno"),
            "feat_media_proficiencia_escola",
            F.col("media_portugues_municipio").alias("feat_media_portugues_municipio"),
            F.col("media_portugues_estado").alias("feat_media_portugues_estado"),
            F.when(F.col("alfabetizado") == True, 1).otherwise(0).alias("target_alfabetizado"),
            F.col("nivel_alfabetizacao").alias("target_nivel_alfabetizacao")
          )
        .withColumn("_gold_processed_at", F.current_timestamp())
  )

def constroi_tabelas_gold(facts, tabelas_gold):
  try:
    for tabela in tabelas_gold.keys():
      log.info("=" * 60)
      log.info(f"[PROC:GOLD] Construindo Tabela: {tabela.upper()}")

      if tabela == "consolidado_municipio":
        tabelas_gold[tabela] = gold_consolidado_municipio(facts)
      elif tabela == "consolidado_uf":
        tabelas_gold[tabela] = gold_consolidado_uf(facts)
      elif tabela == "distribuicao_niveis":
        tabelas_gold[tabela] = gold_distribuicao_niveis(facts)
      elif tabela == "estatisticas_escolares":
        tabelas_gold[tabela] = gold_estatisticas_escolares(facts)
      else:
        tabelas_gold[tabela] = gold_ml_aluno(facts)

      log.info(f"[PROC:GOLD] Sucesso! {tabelas_gold[tabela].count()} registros consolidados.")

    return tabelas_gold

  except Exception as e:
    log.error(f"[PROC:GOLD] Erro ao construir tabela da camada GOLD: {str(e)}")
    raise e

def salvar_camada_gold(df, path_output):
  log.info(f"[GOLD] Salvando em: {path_output}")
  df.write.format("parquet").mode("overwrite").partitionBy("ano").save(path_output)
  log.info(f"[GOLD] {df.count()} registros salvos.")
  return path_output

def checar_qualiade_gold(table, table_name):
  dq_checks = DQ_CHECKS[table_name]
  log.info("=" * 60)
  log.info(f"[DQ:GOLD] Iniciando verificacoes {table_name.upper()} | checks = {len(dq_checks)}")

  for check in dq_checks:
    tipo    = check["tipo"]
    coluna  = check.get("coluna")
    valor   = check.get("valor")
    mensagem_log = ""
    ok      = False

    try:
      if tipo == "not_null":
          nulos   = table.filter(F.col(coluna).isNull()).count()
          ok      = (nulos == 0)
          mensagem_log = f"{nulos} nulos encontrados"
      elif tipo == "min_count":
          contagem = table.count()
          ok       = (contagem >= valor)
          mensagem_log  = f"contagem = {contagem} | minimo = {valor}"
      elif tipo == "unique":
          dups    = table.count() - df.select(coluna).distinct().count()
          ok      = (dups == 0)
          mensagem_log = f"{dups} duplicatas encontradas"
      elif tipo == "range":
          mn, mx = valor
          fora   = table.filter((F.col(coluna) < mn) | (F.col(coluna) > mx)).count()
          ok      = (fora == 0)
          mensagem_log = f"{fora} fora do intervalo [{mn},{mx}]"
    except Exception as e:
      ok      = False
      mensagem_log = f"Erro: {e}"

    if ok:
      log.info(f"[DQ:GOLD] PASS | {tipo} | coluna = {coluna} | {mensagem_log}")
    else:
      log.error(f"[DQ:GOLD] FAIL | {tipo} | coluna = {coluna} | {mensagem_log}")
      raise Exception(f"[DQ:GOLD] Check {coluna}:{tipo} falhou. Job interrompido.")

DQ_CHECKS = {
    "consolidado_municipio": [
        {"tipo": "min_count", "valor": 1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "nome_municipio"},
        {"tipo": "not_null",  "coluna": "rede"},
    ],
    "consolidado_uf": [
        {"tipo": "min_count", "valor": 1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "sigla_uf"},
        {"tipo": "not_null",  "coluna": "rede"},
    ],
    "distribuicao_niveis": [
        {"tipo": "min_count", "valor": 1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "sigla_uf"},
        {"tipo": "not_null",  "coluna": "nome_municipio"},
        {"tipo": "not_null",  "coluna": "rede"},
        {"tipo": "range",     "coluna": "percentual_alunos", "valor": (0, 100)},
    ],
    "estatisticas_escolares": [
        {"tipo": "min_count", "valor": 1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "sigla_uf"},
        {"tipo": "not_null",  "coluna": "nome_municipio"},
        {"tipo": "not_null",  "coluna": "id_escola"},
        {"tipo": "not_null",  "coluna": "rede"},
        {"tipo": "range",     "coluna": "percentual_faltantes", "valor": (0, 100)},
        {"tipo": "range",     "coluna": "media_proficiencia", "valor": (0, 1000)},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao_escola", "valor": (0, 100)},
    ],
    "ml_aluno": [
        {"tipo": "min_count", "valor": 1},
        {"tipo": "not_null",  "coluna": "ano"},
        {"tipo": "not_null",  "coluna": "id_aluno"},
        {"tipo": "not_null",  "coluna": "feat_rede_encoded"},
        {"tipo": "not_null",  "coluna": "feat_peso_aluno"},
        {"tipo": "range",     "coluna": "feat_media_proficiencia_escola", "valor": (0, 1000)},
    ],
}

facts = {
    "fct_avaliacao_aluno": None,
    "fct_meta_municipio": None,
    "fct_meta_uf": None
}

tabelas_gold = {
    "consolidado_municipio": None,
    "consolidado_uf": None,
    "distribuicao_niveis": None,
    "estatisticas_escolares": None,
    "ml_aluno": None
}

log.info("=" * 60)
log.info("INICIANDO PROCESSAMENTO GOLD")
log.info(f"  Lendo de  : {BASE_SILVER}")
log.info(f"  Destino  : {BASE_GOLD}")

try:
  for fact_name in facts.keys():
    log.info("=" * 60)
    log.info(f"[PROC:GOLD] Processando Entidade: {fact_name.upper()}")

    path_input = f"{BASE_SILVER}{fact_name}/_proc_ano={ano}/_proc_mes={mes}/"
    log.info(f"[PROC:GOLD] Lendo dados da Silver em: {path_input}")
    facts[fact_name] = spark.read.parquet(path_input)

    log.info(f"[PROC:GOLD] {facts[fact_name].count()} registros lidos da Silver")

  tabelas_gold = constroi_tabelas_gold(facts, tabelas_gold)

  for nome_tabela, tabela in tabelas_gold.items():
    path_output = f"{BASE_GOLD}{nome_tabela}/"
    checar_qualiade_gold(tabela, nome_tabela)
    gold_path = salvar_camada_gold(tabela, path_output)

    log.info("=" * 60)
    log.info("SUMARIO GOLD")
    log.info(f"  Lido de  : {BASE_SILVER}")
    log.info(f"  Destino  : {gold_path}")
    log.info(f"  Pipeline completo para entidade: {nome_tabela.upper()}")

except Exception as e:
    log.error(f"[PROC:GOLD] Erro ao processar a camada GOLD: {str(e)}")

log.info("=" * 60)
log.info("[PROC:GOLD] Processamento da Camada Gold Finalizado!")

