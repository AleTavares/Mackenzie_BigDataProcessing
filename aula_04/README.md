# Aula 4 — Introdução ao Apache Airflow

**Disciplina:** Big Data Processing — MBA Engenharia de Dados (Mackenzie)

---

## Contexto Narrativo

> **"Cron não escala."**
>
> Os scripts cron começam a falhar silenciosamente. Quando uma etapa quebra, ninguém percebe até o relatório atrasar. Roberto (CEO) exige automação confiável. Carlos propõe Apache Airflow — orquestração com monitoramento, retries e dependências explícitas.

---

## Objetivos de Aprendizagem

Ao final desta aula, o aluno será capaz de:

1. **Compreender** a arquitetura do Airflow (Webserver, Scheduler, Worker, DB)
2. **Criar** DAGs com tasks e dependências explícitas
3. **Usar** PythonOperator e BashOperator
4. **Passar dados** entre tasks com XComs
5. **Configurar** schedules com expressões cron e template variables
6. **Implementar** retry e error handling em tasks

---

## Estrutura da Aula (4 horas)

| Bloco | Conteúdo | Duração |
|-------|----------|---------|
| Teoria | Slides HTML — Airflow, DAGs, Operators, XComs, Schedule | 50 min |
| Intervalo | — | 10 min |
| Lab Parte 1 | Setup Airflow + Primeira DAG + Dependências (guiado) | 60 min |
| Intervalo | — | 10 min |
| Lab Parte 2 | Templates + Retry + Desafio pipeline completo | 50 min |

---

## Estrutura de Arquivos

```
aula_04/
├── README.md                  # Este arquivo
├── aula_04_slides.html        # Slides da teoria (HTML interativo)
├── code/
│   ├── dags/
│   │   ├── dag_vendas_diarias.py  # DAG de exemplo
│   │   └── DESAFIO.md             # Instruções do desafio
│   └── .gitkeep
├── data/
│   └── .gitkeep
└── lab/
    ├── README.md              # Visão geral do lab
    ├── 00_setup.md            # Setup do ambiente Airflow
    ├── 01_primeira_dag.md     # Exercício: criar primeira DAG
    ├── 02_dependencias_xcoms.md   # Exercício: dependências e XComs
    ├── 03_bashoperator_templates.md # Exercício: BashOperator
    ├── 04_schedule_templates.md   # Exercício: cron + Jinja templates
    ├── 05_retry_error_handling.md # Exercício: retries e fallbacks
    ├── 06_desafio_pipeline_completo.md # Desafio: pipeline E2E
    ├── 07_troubleshooting.md  # Guia de problemas
    └── ENTREGAVEL.md
```

---

## Tópicos Abordados

- Apache Airflow: conceitos (DAG, Task, Operator, Sensor)
- Arquitetura: Webserver, Scheduler, Worker, Metadata DB
- PythonOperator e BashOperator
- Dependências entre tasks (`>>`, `<<`, `set_downstream`)
- XComs: passar dados entre tasks
- Schedule intervals (expressões cron)
- Template variables ({{ ds }}, {{ execution_date }})
- Retry, timeout e error handling
- Melhores práticas para DAGs de produção

---

## Rodar no Google Colab (labs com PySpark)

As aulas 4 e 5 focam em **Airflow** (orquestração), que precisa de Docker. Mas os conceitos de DAG podem ser estudados localmente, e os exercícios com PySpark funcionam no Colab:

1. Acesse [colab.research.google.com](https://colab.research.google.com)
2. Adicione esta célula no topo:

```python
# === SETUP COLAB ===
!apt-get install openjdk-17-jdk-headless -qq > /dev/null
!pip install pyspark apache-airflow -q
!git clone https://github.com/AleTavares/Mackenzie_BigDataProcessing.git /content/repo 2>/dev/null
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
DATA_PATH = "/content/repo/datasets/aula_04"
```

> **Nota:** Para o Airflow UI e DAGs rodando, é necessário Docker local. O Colab serve para estudar o código das DAGs e testar funções Python.

---

## Pré-requisitos

- Ter completado as Aulas 1-3 (Spark, DataFrame API, Medallion)
- Docker rodando com pelo menos 8 GB de RAM
- Ambiente com Airflow: `docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d`

---

## Navegação

⬅️ [Aula 3 — Ingestão e Persistência](../aula_03/) · ➡️ [Aula 5 — Orquestração Avançada](../aula_05/)
