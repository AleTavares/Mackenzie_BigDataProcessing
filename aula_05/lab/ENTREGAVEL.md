# Entregável — Informação Importante

## ⚠️ Não há entrega individual nesta aula

O único entregável do curso é o **Projeto Final em Grupo**, apresentado na **Aula 8**.

---

## Sobre o Lab desta Aula

O laboratório desta aula é **prático e guiado**. Você deve:

1. **Executar as DAGs prontas** disponíveis em `code/dags/` no Airflow UI (localhost:8081)
2. **Acompanhar as explicações** nos arquivos do lab para entender cada padrão
3. **Resolver o Desafio** ao final — criar uma DAG avançada com branching e sensors

> O desafio não precisa ser entregue, mas os conceitos praticados aqui serão necessários no Projeto Final.

---

## Projeto Final (Aula 8)

O entregável do curso é um **pipeline de dados end-to-end** em grupo, integrando:

- Apache Spark (PySpark) para processamento
- Apache Airflow para orquestração
- Docker para infraestrutura
- Arquitetura Medallion (Bronze → Silver → Gold)
- Qualidade de dados

📋 Especificação completa: [`aula_08/lab/01_especificacao_projeto_final.md`](../../aula_08/lab/01_especificacao_projeto_final.md)

---

## Como o Lab de Hoje Contribui para o Projeto Final

| Conceito do Lab | Uso no Projeto Final |
|-----------------|---------------------|
| FileSensor | Detectar chegada de dados antes de processar |
| BranchPythonOperator | Lógica condicional no pipeline |
| TaskGroups | Organização visual das tasks |
| SparkSubmitOperator | Integração Airflow → Spark (obrigatória) |
| Callbacks de falha | Monitoramento e alertas |
| trigger_rule | Convergência após branching |
