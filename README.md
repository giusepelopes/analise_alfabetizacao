# Pipeline de Dados para Análise da Alfabetização no Brasil

Este repositório contém a implementação de um pipeline de dados desenvolvido para o **Tech Challenge (Fase 2)** da **Pós-Tech**. O objetivo do projeto é tratar e consolidar dados educacionais associados ao **Compromisso Nacional Criança Alfabetizada**, integrando bases públicas de metas e microdados de desempenho de alunos utilizando uma arquitetura moderna e escalável na nuvem da AWS.

## 1. Contexto do Problema e Desafio Educacional

A alfabetização na infância é um pilar fundamental para o desenvolvimento socioeducacional do país. O **Compromisso Nacional Criança Alfabetizada** estabelece que todas as crianças brasileiras devem estar alfabetizadas até o final do 2º ano do Ensino Fundamental. 

Como parâmetro de referência, a pesquisa *Alfabetiza Brasil* (realizada pelo INEP em 2023) definiu o ponto de corte de **743 pontos** na escala de proficiência do Saeb para que uma criança seja considerada alfabetizada. A meta nacional é alcançar 100% de alfabetização nesse público até 2030.

### O Desafio de Engenharia de Dados
Para compreender os gargalos e as desigualdades do processo de alfabetização, é inviável analisar indicadores isoladamente. Este projeto resolve esse problema unificando e integrando dados altamente heterogêneos:
*   Microdados de desempenho e proficiência de alunos individualizados.
*   Metas nacionais, estaduais e municipais de alfabetização para o período de 2024 a 2030.
*   Dados territoriais e mapeamentos de municípios e UFs.

## 2. Arquitetura da Solução e Fluxo de Dados

A solução foi desenhada seguindo a **Arquitetura Medalhão** (Bronze, Silver e Gold) para garantir rastreabilidade, qualidade e evolução dos dados. Todo o processamento distribuído foi implementado em **PySpark** e implantado de forma serverless na **AWS**.

### Diagrama Conceitual do Fluxo de Dados

1. **LANDING ZONE (S3)**

    ▼ (Job Glue **etl-bronze**: Ingestão e Data Quality Básica)

2. **BRONZE LAYER (S3 Parquet - Raw + Metadados)**
    - Particionado por: _ing_ano / _ing_mes

    ▼ (Job Glue **etl-silver**: Limpeza, Deduplicação e Fatos)

3. **SILVER LAYER (S3 Parquet - Modelo dimensional)**
    - Tabelas Fato: fct_avaliacao_aluno, fct_meta_mun,
fct_meta_uf
    - Particionado por: _proc_ano / _proc_mes

    ▼ (Job Glue **etl-gold**: Agregações e Visões de ML)

4. **GOLD LAYER (S3 Parquet - Dashboards e IA)**
    - Consolidado UF, Municípios, Distribuição de Níveis e Estatísticas Escolares
    - Dataset para Treinamento de Modelos (ml_aluno)
    - Particionado por: ano de referência dos dados

## 3. Tecnologias Utilizadas e Justificativa

*   **Amazon S3**: Utilizado como Data Lake unificado (Landing, Bronze, Silver e Gold). Sua escolha se deve à alta disponibilidade, boa escalabilidade e custo baixo por GB armazenado.
*   **AWS Glue**: Serviço de ETL serverless responsável por rodar os scripts Apache Spark de forma distribuída. Evita a necessidade de gerenciar infraestrutura e escala automaticamente conforme o volume de dados.
*   **Apache Spark (PySpark)**: Motor de processamento ideal para o projeto devido à sua capacidade de lidar com grandes volumes de dados (como os microdados de alunos) utilizando processamento em memória e paralelismo de tarefas.

### Decisões Arquiteturais e Trade-offs
1.  **Batch vs Streaming**: Dados históricos de metas e cadastros territoriais são estáveis e atualizados periodicamente, justificando o processamento **Batch**. Para simular a entrada de novos resultados de avaliações (comportamento **Streaming**), a Landing Zone suporta uploads incrementais contínuos, processados em batches menores via gatilhos de eventos S3.
2.  **Data Lake vs Data Warehouse**: Optou-se por uma estratégia de **Lakehouse híbrido**. O armazenamento e processamento bruto/intermediário ocorrem no S3 para redução de custos, permitindo que apenas os dados consolidados da camada Gold sejam opcionalmente indexados em um Data Warehouse ou acessados diretamente via Amazon Athena.

## 4. Implementação das Camadas e Regras de Negócio

### Camada Bronze (Ingestão & Qualidade Inicial)
*   **Ação**: Lê arquivos CSV brutos da `Landing Zone` e converte para o formato colunar otimizado `Parquet` na `Bronze`.
*   **Metadados Injetados**: Para auditoria e rastreabilidade, são adicionados `_ingestion_timestamp`, `_ingestion_date`, `_source_file`, `_source_entity` e o hash do registro completo `_record_hash`.
*   **Particionamento**: Por ano e mês de ingestão (`_ing_ano` / `_ing_mes`).

### Camada Silver (Tratamento & Modelagem Dimensional)
*   **Deduplicação**: Executa remoção de duplicidade baseada em chaves primárias de negócio específicas de cada entidade.
*   **Casting & Enriquecimento**: 
    *   Tratamento de tipos.
    *   Mapeamento categórico da rede de ensino.
    *   Criação de coluna calculada `nivel_alfabetizacao` com base na pontuação de proficiência.
*   **Unificação de Bases**: Geração de 3 tabelas Fato robustas consolidando dados territoriais e de metas (`fct_avaliacao_aluno`, `fct_meta_municipio` e `fct_meta_uf`).
*   **Particionamento**: Por ano e mês de processamento (`_proc_ano` / `_proc_mes`).

### Camada Gold (Visões Analíticas e Agregações)
*   **Ação**: Gera datasets agregados e estruturados especificamente para consumo final:
    1.  `consolidado_municipio`: Visão de metas versus resultados reais por ano, UF, município e rede, determinando se a meta de alfabetização do ano corrente foi alcançada.
    2.  `consolidado_uf`: Consolidação regional comparando o desempenho médio estadual frente à média nacional de referência.
    3.  `distribuicao_niveis`: Distribuição percentual de estudantes em cada um dos níveis de alfabetização para identificar gargalos geográficos.
    4.  `estatisticas_escolares`: Estatísticas detalhadas por instituição escolar, incluindo total de presentes, percentual de faltas, média e desvio padrão de proficiência, além da taxa de alfabetização da escola.
    5.  `ml_aluno`: Projetada para treinamento de modelos de Machine Learning, fornece features e váriaveis alvo tanto para estratégias de **Classificação** quanto para **Regressão**.
*   **Particionamento**: Por ano e mês de referência dos dados. Escolha feita para otimizar filtros temporais nas consultas dos dashboards e scripts de treino.


## 5. Governança, Qualidade de Dados & FinOps

### Governança e Qualidade (Data Quality)
O pipeline possui verificações de Data Quality embutidas que validam as regras de negócio em tempo de execução. Caso ocorra uma violação grave, o job é abortado impedindo a poluição das camadas subsequentes:
*   **Not Null**: Garante que chaves críticas (como `id_aluno`, `ano` e `id_municipio`) nunca sejam nulas.
*   **Min Count**: Verifica se os arquivos não estão vazios ou corrompidos antes de processar.
*   **Range Checks**: Valida se percentuais e notas de proficiência estão em intervalos plausíveis.

### Otimização de Custos
A arquitetura foi desenhada focando na eficiência orçamentária de nuvem:
*   **Formatos Colunares (Parquet)**: Toda a persistência é feita em Parquet. A alta taxa de compressão diminui o espaço ocupado no S3 e acelera as consultas SQL subsequentes.
*   **Particionamento Inteligente**: Os dados são fisicamente organizados por partições de tempo. Isso evita "Full Table Scans", garantindo que consultas em ferramentas como o Athena analisem apenas os dados do período solicitado, reduzindo drasticamente o custo por query.
*   **Serverless Execution**: O AWS Glue cobra apenas pelos segundos em que o processamento está ativo, eliminando custos de servidores ociosos.

## 6. Aplicações Práticas

*   A camada Gold disponibiliza a tabela `ml_aluno`, projetada especificamente para alimentar pipelines de Machine Learning.

    *   **Features**:
        *   `feat_rede_encoded`: Codificação categórica tratada para modelos numéricos.
        *   `feat_peso_aluno`: Fator de amostragem estatística.
        *   `feat_media_proficiencia_escola`: Média histórica do colégio para capturar o efeito de grupo/vizinhança.
        *   `feat_media_portugues_municipio` e `feat_media_portugues_estado`: Para capturar o contexto geográfico em que o aluno está inserido.
    *   **Variáveis Alvo Disponibilizadas**:
        *   `target_alfabetizado`: Alvo binário para algoritmos de **Classificação** .
        *   `target_nivel_alfabetizacao`: Alvo multiclasse para algoritmos de **Regressão**.

*   As tabelas `consolidado_municipio`, `consolidado_uf` e `distribuicao_niveis` trazem visões macro das taxas de alfabetização, metas esperadas e indicadores de referência, além da distribuição percentual de alunos presentes em cada nível de alfabetização. Prontas para uso em dashboards, essas tabelas permitem analisar o estado atual e a evolução da taxa de alfabetização, buscando identificar desigualdades regionais e disparidades de redes de ensino, e com isso redistribuir a alocação de recursos e elaborar programas de desenvolvimento para as regiões mais críticas.

*   A tabela `estatisticas_escolares` possibilita análises a nível escolar, revelando disparidade entre escolas de um mesmo município e desigualdade interna nos colégios. Além disso, é possível investigar o impacto do percentual de faltantes no déficit de aprendizagem.

## 7. Execução do Projeto

*   Criação de um bucket S3 para armazenar os arquivos brutos e processados.
*   Atribuir o nome do bucket S3 criado à variável S3_BUCKET_NAME, nos arquivos `scripts/etl-bronze.py`, `scripts/etl-silver.py` e `scripts/etl-gold.py`.
*   Carregar os diretórios contidos em `data/s3_folder_structure/` no S3. *OBS.: As bases RAW já estão em* `data/s3_folder_structure/landing_zone/`.
*   Criar o workflow Glue utilizando os scripts Python em `scripts/`, conforme `documentation/aws_glue_workflow.jpeg`.
*   Executar o workflow.

### Fonte de dados: 

**Indicador Criança Alfabetizada - Base dos Dados**
(https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72?table=e1de7a6a-5038-4e81-89f0-a15f2cc12c9b). 

**Último acesso:** 14 de Julho de 2026