# Lab Aula 4 — Introdução ao Apache Airflow

## Visão Geral

Laboratório hands-on da Aula 4, onde os alunos criam suas primeiras DAGs no Apache Airflow. O cenário simula a automação do pipeline de vendas da DataFlow Analytics, substituindo scripts cron por orquestração confiável com monitoramento e retries.

### Objetivos de Aprendizagem

- Configurar ambiente Airflow com Docker Compose
- Criar DAGs com PythonOperator e BashOperator
- Definir dependências entre tasks
- Passar dados entre tasks usando XComs
- Configurar schedules com expressões cron
- Usar template variables ({{ ds }}, {{ execution_date }})
- Implementar retry e error handling

---

## Orçamento de Tempo (Time Budget)

| # | Exercício | Arquivo | Duração |
|---|-----------|---------|---------|
| — | **Lab Parte 1 — Guiado (60 min)** | | |
| 0 | Setup do ambiente Airflow + Docker | `00_setup.md` | 15 min |
| 1 | Primeira DAG: Hello Airflow | `01_primeira_dag.md` | 20 min |
| 2 | Dependências e XComs | `02_dependencias_xcoms.md` | 25 min |
| | **Subtotal Parte 1** | | **60 min** |
| — | **Lab Parte 2 — Intermediário + Desafio (50 min)** | | |
| 3 | BashOperator e templates | `03_bashoperator_templates.md` | 15 min |
| 4 | Schedule intervals e template variables | `04_schedule_templates.md` | 15 min |
| 5 | Retry e error handling | `05_retry_error_handling.md` | 10 min |
| 6 | Desafio: pipeline de vendas completo | `06_desafio_pipeline_completo.md` | 10 min |
| | **Subtotal Parte 2** | | **50 min** |
| | | | |
| | **Total Hands-on** | | **110 min** |

---

## Índice de Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `00_setup.md` | Configuração do ambiente Airflow com Docker Compose |
| `01_primeira_dag.md` | Exercício guiado: criar primeira DAG com PythonOperator |
| `02_dependencias_xcoms.md` | Exercício guiado: dependências entre tasks e XComs |
| `03_bashoperator_templates.md` | Exercício intermediário: BashOperator com Jinja templates |
| `04_schedule_templates.md` | Exercício intermediário: cron schedules e template variables |
| `05_retry_error_handling.md` | Exercício intermediário: retry, timeout e callbacks |
| `06_desafio_pipeline_completo.md` | Desafio: DAG completa para pipeline de vendas |
| `07_troubleshooting.md` | Guia de resolução de problemas comuns |
| `ENTREGAVEL.md` | Especificação do entregável da aula |

---

## Notas para o Instrutor

- O **Setup (15 min)** inclui subir o Airflow — pode ser mais lento na primeira vez (imagens Docker grandes).
- Airflow UI demora ~30s para ficar disponível após o container subir. Peça paciência.
- No **Exercício 2**, XComs costumam gerar dúvidas. Reforce que são key-value com limite de tamanho.
- O **Exercício 6 (desafio)** é opcional — pode ser tarefa de casa.
- Airflow UI: http://localhost:8081 (credenciais: airflow/airflow)
