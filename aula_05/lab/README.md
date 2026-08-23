# Lab Aula 5 — Orquestração Avançada com Airflow

## Visão Geral

Laboratório hands-on da Aula 5, onde os alunos implementam padrões avançados de orquestração: branching condicional, sensors para detectar dados, TaskGroups para organização e integração Airflow + Spark.

### Objetivos de Aprendizagem

- Implementar branching condicional com BranchPythonOperator
- Configurar FileSensor para detectar arquivos antes de processar
- Organizar DAGs com TaskGroups
- Integrar Airflow com Spark via SparkSubmitOperator
- Configurar callbacks de falha e alertas
- Aplicar trigger_rule para controle de fluxo

---

## Orçamento de Tempo (Time Budget)

| # | Exercício | Arquivo | Duração |
|---|-----------|---------|---------|
| — | **Lab Parte 1 — Guiado (60 min)** | | |
| 1 | Branching condicional | `01_branching.md` | 20 min |
| 2 | FileSensor: detectar dados | `02_file_sensor.md` | 20 min |
| 3 | TaskGroups: organização | `03_taskgroups.md` | 20 min |
| | **Subtotal Parte 1** | | **60 min** |
| — | **Lab Parte 2 — Intermediário + Desafio (50 min)** | | |
| 4 | SparkSubmitOperator | `04_spark_submit.md` | 20 min |
| 5 | Callbacks e alertas | `05_callbacks_alertas.md` | 15 min |
| 6 | Desafio: pipeline inteligente | `06_desafio_pipeline.md` | 15 min |
| | **Subtotal Parte 2** | | **50 min** |
| | | | |
| | **Total Hands-on** | | **110 min** |

---

## Índice de Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `01_branching.md` | Exercício guiado: BranchPythonOperator para decisões |
| `02_file_sensor.md` | Exercício guiado: FileSensor para detectar arquivos |
| `03_taskgroups.md` | Exercício guiado: TaskGroups para organizar DAGs |
| `04_spark_submit.md` | Exercício intermediário: SparkSubmitOperator |
| `05_callbacks_alertas.md` | Exercício intermediário: callbacks de falha |
| `06_desafio_pipeline.md` | Desafio: pipeline inteligente com branching + sensor |
| `07_troubleshooting.md` | Guia de resolução de problemas comuns |
| `ENTREGAVEL.md` | Especificação do entregável da aula |

---

## Notas para o Instrutor

- **Branching** é o conceito mais confuso — reforce que apenas UM caminho é seguido por execução.
- O **FileSensor** precisa de um arquivo de teste — crie-o manualmente durante a demonstração.
- **SparkSubmitOperator** pode exigir configuração extra de classpath — consulte troubleshooting.
- Lembre aos alunos que a **formação de grupos** para o projeto final deve ser feita até o final desta aula.
