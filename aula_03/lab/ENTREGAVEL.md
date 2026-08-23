# Entregável — Informação Importante

## ⚠️ Não há entrega individual nesta aula

O único entregável do curso é o **Projeto Final em Grupo**, apresentado na **Aula 8**.

---

## Sobre o Lab desta Aula

O laboratório desta aula é **prático e guiado**. Você deve:

1. **Executar o notebook** disponível em `code/aula03_lab.ipynb` — todo o código está pronto
2. **Acompanhar as explicações** em cada célula para entender os conceitos
3. **Resolver o Desafio** ao final do notebook — este exercício é individual e serve como prática

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
| Ingestão multi-formato (CSV, JSON, Parquet) | Leitura de fontes heterogêneas |
| Arquitetura Medallion (Bronze → Silver → Gold) | Estrutura obrigatória do projeto |
| Metadados de rastreabilidade | Auditoria e reprocessamento |
| Particionamento de dados | Escrita otimizada em Parquet |
| Quarentena de dados inválidos | Quality checks do projeto |
