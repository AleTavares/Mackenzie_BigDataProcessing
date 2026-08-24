# Aula 7 — Pipeline End-to-End em Produção

**Disciplina:** Big Data Processing — MBA Engenharia de Dados (Mackenzie)

---

## Contexto Narrativo

> **"Preparando para produção."**
>
> O pipeline funciona no notebook, mas "funcionar no notebook" não é produção. Marina exige: scripts parametrizados, logging estruturado, escrita idempotente, orquestração automática e tudo containerizado. É hora de montar o pipeline completo.

---

## Objetivos de Aprendizagem

Ao final desta aula, o aluno será capaz de:

1. **Converter** notebooks em scripts de produção com CLI (argparse)
2. **Implementar** logging estruturado (JSON) para observabilidade
3. **Garantir** escrita idempotente (partition overwrite dinâmico)
4. **Orquestrar** pipeline completo via DAG Airflow
5. **Containerizar** o ambiente (Spark + Airflow + Jupyter via Docker Compose)
6. **Integrar** quality checks no pipeline de produção

---

## Estrutura da Aula (4 horas)

| Bloco | Conteúdo | Duração |
|-------|----------|---------|
| Teoria | Slides HTML — Notebook→Produção, Logging, Idempotência, Docker | 50 min |
| Intervalo | — | 10 min |
| Lab Parte 1 | Containerização + Logging + Escrita idempotente (guiado) | 60 min |
| Intervalo | — | 10 min |
| Lab Parte 2 | DAG completa + Quality checks + Desafio E2E | 50 min |

---

## Estrutura de Arquivos

```
aula_07/
├── README.md                  # Este arquivo
├── aula_07_slides.html        # Slides da teoria (HTML interativo)
├── code/
│   ├── dags/
│   │   └── dag_pipeline_vendas.py    # DAG de produção
│   ├── spark_jobs/
│   │   ├── pipeline_vendas.py        # Script Spark de produção
│   │   └── structured_logging.py     # Módulo de logging
│   ├── DESAFIO.md                     # Instruções do desafio
│   └── .gitkeep
├── data/
│   └── .gitkeep
└── lab/
    ├── README.md              # Visão geral do lab
    ├── 01_containerizar_spark_job.md  # Exercício: notebook → script
    ├── 02_logging_estruturado.md      # Exercício: logging JSON
    ├── 03_escrita_idempotente.md      # Exercício: partition overwrite
    ├── 04_dag_orquestracao.md         # Exercício: DAG completa
    ├── 05_quality_checks_dag.md       # Exercício: quality gate na DAG
    ├── 06_desafio_pipeline_e2e.md     # Desafio: pipeline E2E completo
    ├── 07_troubleshooting.md          # Guia de problemas
    └── ENTREGAVEL.md
```

---

## Tópicos Abordados

- Notebook vs Produção: por que notebooks não escalam
- Scripts com argparse/CLI: parametrização
- Logging estruturado (JSON): timestamp, level, message, context
- Escrita idempotente: partition overwrite dinâmico
- Docker Compose: stack completa (Spark + Airflow + Jupyter)
- DAG de produção: sensor → spark_submit → quality_check → notify
- Quality gates: bloquear pipeline se dados falharem validação
- Testes de integração: validar pipeline end-to-end

---

## Rodar no Google Colab (parcial)

A Aula 7 integra Spark + Airflow + Docker — o pipeline completo precisa de Docker local. Mas os exercícios de PySpark (logging, escrita idempotente) funcionam no Colab:

1. Acesse [colab.research.google.com](https://colab.research.google.com)
2. Adicione esta célula no topo:

```python
# === SETUP COLAB ===
!apt-get install openjdk-17-jdk-headless -qq > /dev/null
!pip install pyspark -q
!git clone https://github.com/AleTavares/Mackenzie_BigDataProcessing.git /content/repo 2>/dev/null
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
DATA_PATH = "/content/repo/datasets/aula_07"
```

> **Nota:** A integração Airflow + Docker Compose precisa de ambiente local. No Colab, foque nos exercícios 01-03 (Spark puro).

---

## Pré-requisitos

- Ter completado as Aulas 1-6 (Spark + Airflow + Qualidade)
- Docker rodando com pelo menos 8 GB de RAM
- Stack completa: `docker compose -f shared/docker-compose.full.yml up -d`

---

## Navegação

⬅️ [Aula 6 — Qualidade de Dados](../aula_06/) · ➡️ [Aula 8 — Projeto Final](../aula_08/)
