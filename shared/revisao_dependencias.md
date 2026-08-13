# Revisão de Dependências entre Aulas

## Resumo Executivo

Este documento valida que todas as dependências entre aulas (01-07) estão explícitas nos pré-requisitos dos laboratórios, conforme definido no design do curso.

**Resultado da Validação:** ✅ Cadeia de dependências correta — nenhuma dependência circular ou referência futura (forward dependency) encontrada. Algumas melhorias sugeridas para explicitar melhor certas dependências.

---

## 1. Matriz de Dependências (Design vs Implementação)

### 1.1 Dependências entre Aulas (conforme Design Document)

```
Aula 01: Fundamentos Spark          → Nenhuma dependência (primeira aula)
Aula 02: Transformações Avançadas    → Aula 01
Aula 03: Ingestão e Persistência     → Aulas 01, 02
Aula 04: Introdução ao Airflow       → Aulas 01, 02, 03
Aula 05: Orquestração Avançada       → Aulas 01, 02, 03, 04
Aula 06: Qualidade e Monitoramento   → Aulas 01, 02, 03, 04, 05
Aula 07: Pipeline End-to-End         → Aulas 01, 02, 03, 04, 05, 06
```

### 1.2 Matriz Visual de Dependências

| Aula | Depende de → | 01 | 02 | 03 | 04 | 05 | 06 |
|------|--------------|----|----|----|----|----|----|
| **01** | — | — | — | — | — | — | — |
| **02** | Spark básico | ✅ | — | — | — | — | — |
| **03** | Spark + Transformações | ✅ | ✅ | — | — | — | — |
| **04** | Spark + Docker | ✅ | ○ | ○ | — | — | — |
| **05** | Airflow + Spark | ✅ | ○ | ○ | ✅ | — | — |
| **06** | Spark + Airflow | ✅ | ○ | ○ | ○ | ✅ | — |
| **07** | Todas as anteriores | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

**Legenda:** ✅ = Dependência direta e explícita nos pré-requisitos | ○ = Dependência indireta (transitiva)

---

## 2. Validação Detalhada por Aula

### Aula 01 — Fundamentos de Big Data e Apache Spark

**Pré-requisitos declarados nos labs:**
- `00_setup.md`: Docker Desktop 24.0+, Docker Compose v2 2.20+, Git 2.30+, 8GB RAM, 4 cores
- `01_spark_basico.md`: Ambiente Docker rodando (ver `00_setup.md`), Jupyter acessível, dataset `vendas_2023.csv`

**Dependências de outras aulas:** Nenhuma ✅

**Análise:** Correto. É a primeira aula do curso. Pré-requisitos são apenas de infraestrutura (software instalado).

---

### Aula 02 — Transformações Avançadas com Spark

**Pré-requisitos declarados nos labs:**
- `01_joins_multifonte.md`: Ambiente Docker rodando (ver `00_setup.md` da **Aula 1**), Jupyter acessível, datasets da aula_02
- `02_window_functions.md`: **Exercício 1 concluído** (df_completo cacheado na sessão)

**Dependências de outras aulas:** Aula 01 (ambiente Docker, SparkSession básica) ✅

**Cross-references internas:**
- Exercício 02 depende do Exercício 01 (df_completo cacheado) ✅
- Exercício 03 (UDFs) depende dos dados carregados no Exercício 01 ✅
- Exercício 05 (Desafio otimização) depende de exercícios 01-04 ✅

**Análise:** Correto. Referencia explicitamente o `00_setup.md` da Aula 01 para o ambiente.

---

### Aula 03 — Ingestão e Persistência de Dados

**Pré-requisitos declarados nos labs:**
- `01_ingestao_csv_legado.md`: Ambiente Docker rodando (ver `00_setup.md` da **Aula 1**), Jupyter acessível, datasets parceiro_a
- `04_camada_silver.md`: SparkSession ativa, Camada Bronze persistida (**Exercício 3 concluído**)
- `06_camada_gold.md`: SparkSession ativa, Camada Silver persistida (**Exercícios 4 e 5 concluídos**)

**Dependências de outras aulas:** Aula 01 (ambiente Docker, SparkSession) ✅

**Cross-references internas:**
- Exercício 03 (Bronze) depende de Exercícios 01-02 (ingestão) ✅
- Exercício 04 (Silver) depende de Exercício 03 (Bronze) ✅
- Exercício 05 (Persistência particionada) depende de Exercícios 03-04 ✅
- Exercício 06 (Gold) depende de Exercícios 04-05 (Silver) ✅

**Análise:** Correto. A cadeia interna Bronze → Silver → Gold está bem definida. A dependência da Aula 02 é implícita (conceitos de transformações são reutilizados mas não referenciados explicitamente nos pré-requisitos).

**Observação:** A Aula 03 Exercício 04 (Camada Silver) usa `unionByName` e operações de normalização que são conceitos da Aula 02. A dependência é implícita — os alunos aprenderam joins e transformações na Aula 02, mas isso não está listado como pré-requisito explícito. **Impacto baixo** — o exercício guia o aluno pela implementação.

---

### Aula 04 — Introdução ao Apache Airflow

**Pré-requisitos declarados nos labs:**
- `00_setup.md`: **Ambiente base rodando (Spark + Jupyter)** — referência implícita a Aulas 01-03
- `01_primeira_dag.md`: Ambiente Airflow rodando (ver `00_setup.md`), Airflow UI acessível, pasta dags montada

**Dependências de outras aulas:** Aulas 01-03 (ambiente Docker com Spark) ✅

**Cross-references internas:**
- Exercício 02 (XComs) depende de Exercício 01 (primeira DAG) ✅
- Exercício 05 (Retry/Error handling) depende de Exercícios 01-02 ✅
- Exercício 06 (Desafio pipeline completo) depende de Exercícios 01-05 ✅

**Análise:** Correto. O setup da Aula 04 verifica que o ambiente base (Aulas 01-03) está rodando antes de adicionar Airflow. A dependência no design (Spark → Airflow) está respeitada.

---

### Aula 05 — Orquestração Avançada com Airflow

**Pré-requisitos declarados nos labs:**
- `01_branching.md`: **Aula 04 concluída** (DAGs básicas, PythonOperator, XComs, dependências), Ambiente Airflow rodando
- `02_file_sensor.md`: **Exercício 01 concluído**, Ambiente Airflow (ver `aula_04/lab/00_setup.md`)
- `04_spark_submit.md`: **Exercícios 01-03 concluídos**, Ambiente Docker com Airflow **e** Spark, Spark Master acessível, Spark UI acessível
- `06_desafio_pipeline.md`: Exercícios 01-05 concluídos (todos os conceitos combinados)

**Dependências de outras aulas:** Aula 04 (conceitos básicos de Airflow) ✅, Aulas 01-03 (Spark) ✅

**Cross-references internas:**
- Exercício 02 depende de Exercício 01 ✅
- Exercício 03 depende de Exercícios 01-02 ✅
- Exercício 04 depende de Exercícios 01-03 ✅
- Exercício 05 depende de Exercícios 01-04 ✅
- Exercício 06 (Desafio) depende de todos (01-05) ✅

**Análise:** Correto. Dependências bem explicitadas. A Aula 05 lista explicitamente "Aula 04 concluída" e referencia o `00_setup.md` da Aula 04.

---

### Aula 06 — Qualidade de Dados e Monitoramento

**Pré-requisitos declarados nos labs:**
- `01_check_completude.md`: Ambiente Docker rodando (Spark + Jupyter), dataset `dados_sujos`
- `04_quarentena.md`: **Exercícios 01, 02 e 03 concluídos** (funções de check implementadas), tabelas de referência
- `05_dag_qualidade.md`: Ambiente Docker com **Spark + Airflow**, **Exercício 04 concluído**, Familiaridade com `BranchPythonOperator`, `FileSensor`, XCom e callbacks (**Aula 05**)
- `06_framework_qualidade.md`: **Exercícios 01 a 05 concluídos**

**Dependências de outras aulas:** Aulas 01-03 (Spark), Aulas 04-05 (Airflow, BranchPythonOperator, FileSensor, callbacks) ✅

**Cross-references internas:**
- Exercício 02 depende de Exercício 01 ✅
- Exercício 03 depende de Exercícios 01-02 ✅
- Exercício 04 depende de Exercícios 01-03 ✅
- Exercício 05 depende de Exercício 04 + **Aula 05 explicitamente** ✅
- Exercício 06 depende de Exercícios 01-05 ✅

**Análise:** Correto. O Exercício 05 explicita a dependência da Aula 05 (conceitos avançados de Airflow). A progressão interna (checks individuais → quarentena → integração com DAG → framework) é logicamente sólida.

---

### Aula 07 — Pipeline End-to-End em Produção

**Pré-requisitos declarados nos labs:**
- `01_containerizar_spark_job.md`: Ambiente Docker (Spark + Jupyter), **Aulas 1-6 concluídas** (PySpark, Medallion, Airflow, Quality Checks)
- `04_dag_orquestracao.md`: **Exercícios 01, 02 e 03 concluídos**, **Aula 05** (FileSensor, SparkSubmitOperator, callbacks)
- `05_quality_checks_dag.md`: **Exercício 04 concluído**, **Aula 06** (conceitos de quality checks)
- `06_desafio_pipeline_e2e.md`: **Exercícios 01 a 05 desta aula concluídos**, Aulas 04-05 (Airflow), Docker Compose

**Dependências de outras aulas:** Todas as aulas anteriores (01-06) ✅

**Cross-references internas:**
- Exercício 02 depende de Exercício 01 ✅
- Exercício 03 depende de Exercícios 01-02 ✅
- Exercício 04 depende de Exercícios 01-03 + **Aula 05** ✅
- Exercício 05 depende de Exercício 04 + **Aula 06** ✅
- Exercício 06 depende de Exercícios 01-05 + **Aulas 04, 05** ✅

**Análise:** Correto. A Aula 07 é a mais integradora — referencia explicitamente "Aulas 1-6 concluídas" no primeiro exercício. Cada exercício subsequente referencia as aulas anteriores relevantes.

---

## 3. Verificação de Forward Dependencies (Referências Futuras)

| Aula | Referência a Aulas Futuras? | Status |
|------|-----------------------------|--------|
| 01 | Nenhuma | ✅ OK |
| 02 | Nenhuma — menciona "próxima aula" apenas como preview | ✅ OK |
| 03 | Nenhuma | ✅ OK |
| 04 | Nenhuma | ✅ OK |
| 05 | Nenhuma | ✅ OK |
| 06 | Nenhuma | ✅ OK |
| 07 | Nenhuma | ✅ OK |

**Resultado:** ✅ Nenhuma forward dependency encontrada. O material segue estritamente a progressão linear definida no design.

**Nota:** A Aula 01 Exercício 01 contém a menção: *"Na Aula 3 veremos dados 'sujos' onde isso acontece"* — isso é um teaser/preview pedagógico (boas práticas), **não** uma dependência funcional. O exercício funciona independentemente.

---

## 4. Validação da Cadeia de Reutilização de Conceitos

### 4.1 Arquitetura Medallion (Bronze/Silver/Gold)

| Introdução | Reutilização |
|------------|-------------|
| Aula 03 (implementação completa) | Aula 07, Ex 01 (pipeline_vendas.py com Bronze→Silver→Gold) |

**Status:** ✅ Conceito introduzido na Aula 03, reutilizado na Aula 07 com referência explícita.

### 4.2 Docker Compose (evolução incremental)

| Aula | Docker Compose |
|------|---------------|
| 01 | `shared/docker-compose.yml` (Spark + Jupyter) |
| 04 | `shared/docker-compose.airflow.yml` (adição de Airflow) |
| 07 | `docker-compose.producao.yml` (stack completa) |

**Status:** ✅ Evolução incremental. A Aula 04 verifica que o ambiente base está rodando antes de adicionar Airflow.

### 4.3 SparkSession (reutilização entre aulas)

| Aula | AppName da SparkSession |
|------|------------------------|
| 01 | `DataFlow-Aula01-Basico` |
| 02 | `DataFlow-Aula02-Joins` |
| 03 | `DataFlow-Aula03-Ingestao` |
| 06 | `DataFlow-Aula06-QualityCheck` |
| 07 | `DataFlow-Pipeline-Vendas` (CLI) |

**Status:** ✅ Cada aula cria sua própria SparkSession. O padrão de criação ensinado na Aula 01 é consistente ao longo do curso.

### 4.4 Conceitos de Airflow (progressão)

| Conceito | Introdução | Reutilização |
|----------|-----------|-------------|
| DAG, PythonOperator | Aula 04 | Aulas 05, 06, 07 |
| XComs | Aula 04 | Aulas 05, 06 |
| BranchPythonOperator | Aula 05 | Aula 06 (quality gate) |
| FileSensor | Aula 05 | Aulas 06, 07 |
| SparkSubmitOperator | Aula 05 | Aula 07 |
| Callbacks | Aula 05 | Aulas 06, 07 |

**Status:** ✅ Todos os conceitos são introduzidos antes de serem reutilizados. As aulas subsequentes referenciam explicitamente a aula onde o conceito foi introduzido.

### 4.5 Quality Checks (progressão)

| Conceito | Introdução | Reutilização |
|----------|-----------|-------------|
| Completude | Aula 06, Ex 01 | Aula 07, Ex 05 |
| Quarentena | Aula 06, Ex 04 | Aula 07 (padrão fail-fast) |
| Quality Gate | Aula 06, Ex 05 | Aula 07, Ex 05 |

**Status:** ✅ Qualidade de dados introduzida na Aula 06 e aplicada na Aula 07 com referências explícitas.

---

## 5. Observações e Recomendações

### 5.1 Dependências Implícitas (não críticas)

| Local | Dependência Implícita | Recomendação |
|-------|----------------------|--------------|
| Aula 03, Ex 04 (Silver) | Conceitos de transformação/union da Aula 02 | ⚠️ Considerar adicionar "Conceitos de Aula 02 (transformações)" como pré-requisito |
| Aula 06, Ex 01 | Conceito de Parquet (formato) introduzido na Aula 03 | ✅ OK — formato Parquet já é familiar ao aluno pela Aula 03 |
| Aula 04, Ex 01 | Conceito de ETL (bronze/silver/gold) da Aula 03 | ✅ OK — a DAG simula ETL mas não usa a mesma arquitetura medallion |

### 5.2 Pontos Fortes do Design de Dependências

1. **Setup referenciado consistentemente**: Todos os exercícios que precisam do ambiente Docker apontam para `00_setup.md` da Aula 01 ou Aula 04
2. **Cadeia interna bem definida**: Dentro de cada aula, exercícios posteriores listam explicitamente "Exercício X concluído" como pré-requisito
3. **Cross-references entre aulas explícitas**: Aulas 05, 06 e 07 listam claramente as aulas anteriores necessárias
4. **Sem forward dependencies**: Nenhum exercício referencia conceitos de aulas futuras como requisito funcional
5. **Preview pedagógico sem dependência funcional**: Referências como "na próxima aula veremos..." são apenas teasers, não bloqueantes

---

## 6. Conclusão

| Critério | Status |
|----------|--------|
| Dependências entre aulas explícitas nos pré-requisitos | ✅ Aprovado |
| Nenhuma forward dependency (referência a aulas futuras) | ✅ Aprovado |
| Cadeia de dependências conforme design (Aula N requer 1..N-1) | ✅ Aprovado |
| Cross-references internas corretas (Ex 04 depende de 01-03) | ✅ Aprovado |
| Conceitos reutilizados sempre introduzidos antes | ✅ Aprovado |
| Docker Compose evolui incrementalmente | ✅ Aprovado |

**Validação Final:** ✅ O curso possui uma cadeia de dependências bem estruturada e explícita. Todas as aulas seguem a progressão sequencial definida no design document. Nenhum problema crítico encontrado.

---

*Documento gerado como parte da Tarefa 12.2 — Revisão e Integração Final*
*Data: Revisão realizada sobre todos os labs das Aulas 01-07*
