# Lab Aula 7 — Pipeline End-to-End em Produção

## Visão Geral

Laboratório hands-on da Aula 7, onde os alunos integram tudo que aprenderam num pipeline de produção completo: script Spark parametrizado, logging estruturado, escrita idempotente, orquestração Airflow e quality checks — tudo containerizado.

### Objetivos de Aprendizagem

- Converter notebook em script de produção com argparse
- Implementar logging estruturado (JSON) para observabilidade
- Configurar escrita idempotente com partition overwrite dinâmico
- Montar DAG Airflow completa (sensor → spark → quality → notify)
- Integrar quality checks como gate no pipeline
- Validar pipeline end-to-end via Docker Compose

---

## Orçamento de Tempo (Time Budget)

| # | Exercício | Arquivo | Duração |
|---|-----------|---------|---------|
| — | **Lab Parte 1 — Guiado (60 min)** | | |
| 1 | Containerizar Spark job | `01_containerizar_spark_job.md` | 20 min |
| 2 | Logging estruturado (JSON) | `02_logging_estruturado.md` | 20 min |
| 3 | Escrita idempotente | `03_escrita_idempotente.md` | 20 min |
| | **Subtotal Parte 1** | | **60 min** |
| — | **Lab Parte 2 — Intermediário + Desafio (50 min)** | | |
| 4 | DAG de orquestração completa | `04_dag_orquestracao.md` | 15 min |
| 5 | Quality checks na DAG | `05_quality_checks_dag.md` | 15 min |
| 6 | Desafio: pipeline E2E completo | `06_desafio_pipeline_e2e.md` | 20 min |
| | **Subtotal Parte 2** | | **50 min** |
| | | | |
| | **Total Hands-on** | | **110 min** |

---

## Índice de Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `01_containerizar_spark_job.md` | Exercício guiado: notebook → script com argparse |
| `02_logging_estruturado.md` | Exercício guiado: logging JSON para produção |
| `03_escrita_idempotente.md` | Exercício guiado: partition overwrite dinâmico |
| `04_dag_orquestracao.md` | Exercício intermediário: DAG completa Airflow |
| `05_quality_checks_dag.md` | Exercício intermediário: quality gate no pipeline |
| `06_desafio_pipeline_e2e.md` | Desafio: pipeline completo (Spark + Airflow + Docker) |
| `07_troubleshooting.md` | Guia de resolução de problemas comuns |
| `ENTREGAVEL.md` | Especificação do entregável da aula |

---

## Notas para o Instrutor

- Esta é a aula de **integração** — tudo junto. Alunos que ficaram para trás podem ter dificuldade.
- O **Exercício 1** é a transição mais importante: notebook → script. Dedique tempo para explicar a motivação.
- O **Exercício 4** (DAG) requer que Spark e Airflow estejam rodando simultaneamente.
- O **Desafio** é essencialmente um mini-projeto final — excelente preparação para a Aula 8.
- Lembre aos alunos: o repositório do projeto final deve ser entregue 48h antes da Aula 8.
