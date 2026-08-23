# Aula 5 — Orquestração Avançada com Airflow

**Disciplina:** Big Data Processing — MBA Engenharia de Dados (Mackenzie)

---

## Contexto Narrativo

> **"O pipeline precisa ser inteligente."**
>
> Nem todo dia é igual: segundas têm volume alto (acúmulo do fim de semana), sextas precisam de relatório semanal. O pipeline precisa tomar decisões automaticamente baseado no dia, no volume de dados disponível e em condições externas.

---

## Objetivos de Aprendizagem

Ao final desta aula, o aluno será capaz de:

1. **Implementar** branching condicional com BranchPythonOperator
2. **Configurar** Sensors (FileSensor, ExternalTaskSensor) para detectar condições
3. **Organizar** DAGs complexas com TaskGroups
4. **Integrar** Airflow com Spark via SparkSubmitOperator
5. **Configurar** callbacks de falha e alertas
6. **Aplicar** trigger_rule para controle de fluxo avançado

---

## Estrutura da Aula (4 horas)

| Bloco | Conteúdo | Duração |
|-------|----------|---------|
| Teoria | Slides HTML — Branching, Sensors, TaskGroups, SparkSubmit | 50 min |
| Intervalo | — | 10 min |
| Lab Parte 1 | Branching + FileSensor + TaskGroups (guiado) | 60 min |
| Intervalo | — | 10 min |
| Lab Parte 2 | SparkSubmit + Callbacks + Desafio | 50 min |

---

## Estrutura de Arquivos

```
aula_05/
├── README.md                  # Este arquivo
├── aula_05_slides.html        # Slides da teoria (HTML interativo)
├── code/
│   ├── dags/
│   │   ├── dag_branching_processamento.py  # DAG com branching
│   │   ├── dag_sensor_arquivo.py           # DAG com FileSensor
│   │   ├── dag_taskgroups_multi_fonte.py   # DAG com TaskGroups
│   │   └── DESAFIO.md                      # Instruções do desafio
│   └── .gitkeep
├── data/
│   └── .gitkeep
└── lab/
    ├── README.md              # Visão geral do lab
    ├── 01_branching.md        # Exercício: BranchPythonOperator
    ├── 02_file_sensor.md      # Exercício: FileSensor
    ├── 03_taskgroups.md       # Exercício: TaskGroups
    ├── 04_spark_submit.md     # Exercício: SparkSubmitOperator
    ├── 05_callbacks_alertas.md # Exercício: callbacks e alertas
    ├── 06_desafio_pipeline.md # Desafio: pipeline inteligente
    ├── 07_troubleshooting.md  # Guia de problemas
    └── ENTREGAVEL.md
```

---

## Tópicos Abordados

- BranchPythonOperator (branching condicional)
- Sensors: FileSensor, ExternalTaskSensor
- TaskGroups para organização de DAGs complexas
- SparkSubmitOperator: integração Airflow + Spark
- Callbacks de falha (on_failure_callback)
- trigger_rule: one_success, all_done, none_failed
- Padrões: idempotência, retry exponencial, dead letter queue

---

## Pré-requisitos

- Ter completado a Aula 4 (conceitos básicos de Airflow, DAGs, Operators)
- Docker rodando com pelo menos 8 GB de RAM
- Ambiente com Airflow + Spark ativo

---

## Navegação

⬅️ [Aula 4 — Introdução ao Airflow](../aula_04/) · ➡️ [Aula 6 — Qualidade de Dados](../aula_06/)
