# Entregável — Informação Importante

## ⚠️ Não há entrega individual nesta aula

O único entregável do curso é o **Projeto Final em Grupo**, apresentado na **Aula 8**.

---

## Sobre o Lab desta Aula

O laboratório desta aula é **prático e guiado**. Você deve:

1. **Executar o pipeline pronto** disponível em `code/` (script Spark + DAG + Docker Compose)
2. **Acompanhar as explicações** nos arquivos do lab para entender a integração
3. **Resolver o Desafio** ao final — adaptar o pipeline para um cenário diferente

> O desafio não precisa ser entregue, mas este lab é o mais próximo do Projeto Final. Use-o como template.

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
| Containerização de Spark jobs | Script .py com argparse (obrigatório) |
| Logging estruturado | Observabilidade do pipeline |
| Escrita idempotente | Re-execução sem duplicação |
| DAG de orquestração completa | Integração Airflow + Spark |
| Quality checks na DAG | Validação automática antes de notificar |
| Docker Compose end-to-end | Reprodutibilidade (docker compose up) |

> **Dica:** O pipeline desta aula é essencialmente o "esqueleto" do Projeto Final. Adapte-o para a vertical do seu grupo.
