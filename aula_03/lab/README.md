# Lab Aula 3 — Ingestão e Persistência de Dados

## Visão Geral

Laboratório hands-on da Aula 3, onde os alunos constroem um pipeline de ingestão multi-formato implementando a Arquitetura Medallion (Bronze → Silver → Gold). O cenário simula a integração de 3 novos parceiros da DataFlow Analytics, cada um com formato e peculiaridades diferentes.

### Objetivos de Aprendizagem

- Ingerir dados de múltiplos formatos (CSV legado, JSON nested, Parquet)
- Tratar encoding (ISO-8859-1), separadores e formatos de data não-padrão
- Implementar camada Bronze com metadados de rastreabilidade
- Normalizar schemas de fontes heterogêneas na camada Silver
- Persistir dados com particionamento otimizado
- Criar camada Gold com agregações de negócio

---

## Orçamento de Tempo (Time Budget)

| # | Exercício | Arquivo | Duração |
|---|-----------|---------|---------|
| — | **Lab Parte 1 — Guiado (60 min)** | | |
| 1 | Ingestão de CSV legado (Parceiro A) | `01_ingestao_csv_legado.md` | 20 min |
| 2 | Ingestão de JSON e Parquet (Parceiros B e C) | `02_ingestao_json_parquet.md` | 20 min |
| 3 | Camada Bronze com metadados | `03_camada_bronze.md` | 20 min |
| | **Subtotal Parte 1** | | **60 min** |
| — | **Lab Parte 2 — Intermediário + Desafio (50 min)** | | |
| 4 | Camada Silver: normalização e validação | `04_camada_silver.md` | 15 min |
| 5 | Persistência particionada | `05_persistencia_particionada.md` | 15 min |
| 6 | Desafio: Camada Gold + Schema Evolution | `06_camada_gold.md` | 20 min |
| | **Subtotal Parte 2** | | **50 min** |
| | | | |
| | **Total Hands-on** | | **110 min** |

---

## Índice de Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `01_ingestao_csv_legado.md` | Exercício guiado: CSV com encoding ISO-8859-1, separador `;`, datas brasileiras |
| `02_ingestao_json_parquet.md` | Exercício guiado: JSON nested com explode + Parquet particionado |
| `03_camada_bronze.md` | Exercício guiado: implementar Bronze com metadados de ingestão |
| `04_camada_silver.md` | Exercício intermediário: normalização de schema, deduplicação, validação |
| `05_persistencia_particionada.md` | Exercício intermediário: particionamento por data, partition pruning |
| `06_camada_gold.md` | Exercício desafio: agregações Gold + schema evolution |
| `07_troubleshooting.md` | Guia de resolução de problemas comuns |
| `ENTREGAVEL.md` | Especificação do entregável da aula |

---

## Datasets Utilizados

| Dataset | Formato | Fonte | Localização |
|---------|---------|-------|-------------|
| vendas_legacy_*.csv | CSV (ISO-8859-1, sep=`;`) | Parceiro A (ERP legado) | `datasets/aula_03/parceiro_a/` |
| api_dump_page_*.json | JSON (nested arrays) | Parceiro B (API REST) | `datasets/aula_03/parceiro_b/` |
| vendas_*.parquet | Parquet (Snappy) | Parceiro C (Data Lake) | `datasets/aula_03/parceiro_c/` |

---

## Notas para o Instrutor

- O **Exercício 1** é onde mais alunos travam — encoding ISO-8859-1 e separador `;` geram erros confusos. Circule pela sala.
- No **Exercício 2**, explode() de JSON nested é um conceito novo — reforce com diagrama no quadro.
- Na **Parte 2**, o particionamento e schema evolution são progressivamente mais abertos.
- O **Exercício 6 (desafio)** pode ser atribuído como tarefa de casa se o tempo estiver curto.
- Consulte `07_troubleshooting.md` para erros comuns (encoding, schema mismatch, small files).
- Reserve os últimos 2-3 min de cada parte para dúvidas.
