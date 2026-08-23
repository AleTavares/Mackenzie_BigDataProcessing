# Entregável — Informação Importante

## ⚠️ Não há entrega individual nesta aula

O único entregável do curso é o **Projeto Final em Grupo**, apresentado na **Aula 8**.

---

## Sobre o Lab desta Aula

O laboratório desta aula é **prático e guiado**. Você deve:

1. **Executar o notebook** disponível em `code/aula06_lab.ipynb` — todo o código está pronto
2. **Acompanhar as explicações** em cada célula para entender os conceitos
3. **Resolver o Desafio** ao final do notebook — implementar checks adicionais

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
| Checks de completude | Mínimo de 3 checks obrigatórios |
| Checks de unicidade | Validação de chaves primárias |
| Integridade referencial | Verificar foreign keys entre tabelas |
| Sistema de quarentena | Separar dados inválidos (requisito) |
| Relatório de qualidade | Métricas de saúde do pipeline |
| Princípio de conservação | Garantir nenhum dado perdido |
