# Lab Aula 2 — Transformações Avançadas com Spark

## Visão Geral

Laboratório hands-on da Aula 2, onde os alunos dominam joins distribuídos, window functions, UDFs e análise de planos de execução usando PySpark. O cenário simula a preparação de relatórios consolidados para a Black Friday da DataFlow Analytics, cruzando 3 fontes de dados (vendas, clientes e categorias).

### Objetivos de Aprendizagem

- Executar joins multi-fonte (inner, left, broadcast, anti) em DataFrames grandes
- Verificar planos de execução com `explain()` e identificar estratégias de join
- Aplicar Window Functions para rankings, tendências temporais e totais acumulados
- Criar UDFs e Pandas UDFs, compreendendo o impacto na performance
- Otimizar queries com cache, broadcast e particionamento

---

## Orçamento de Tempo (Time Budget)

| # | Exercício | Arquivo | Duração |
|---|-----------|---------|---------|
| — | **Lab Parte 1 — Guiado (60 min)** | | |
| 0 | Setup do ambiente e verificação dos datasets | `00_setup.md` | 10 min |
| 1 | Joins com múltiplas fontes de dados | `01_joins_multifonte.md` | 25 min |
| 2 | Window Functions: ranking e tendências | `02_window_functions.md` | 25 min |
| | **Subtotal Parte 1** | | **60 min** |
| — | **Lab Parte 2 — Intermediário + Desafio (50 min)** | | |
| 3 | UDFs e análise com explain() | `03_udfs_explain.md` | 15 min |
| 4 | Análise de plano de execução | `04_analise_plano_execucao.md` | 15 min |
| 5 | Desafio: otimização de query | `05_desafio_otimizacao.md` | 20 min |
| | **Subtotal Parte 2** | | **50 min** |
| | | | |
| | **Total Hands-on** | | **110 min** |

---

## Índice de Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `00_setup.md` | Configuração do ambiente Docker + verificação dos datasets |
| `01_joins_multifonte.md` | Exercício guiado: joins multi-fonte (inner, left, broadcast, anti) |
| `02_window_functions.md` | Exercício guiado: row_number, rank, dense_rank, lag, lead, running totals |
| `03_udfs_explain.md` | Exercício intermediário: UDFs, Pandas UDFs e impacto de performance |
| `04_analise_plano_execucao.md` | Exercício intermediário: leitura e interpretação de planos de execução |
| `05_desafio_otimizacao.md` | Exercício desafio: otimizar pipeline com broadcast + cache + coalesce |
| `06_troubleshooting.md` | Guia de resolução de problemas comuns |
| `ENTREGAVEL.md` | Especificação do entregável da aula |

---

## Datasets Utilizados

| Dataset | Formato | Registros | Localização |
|---------|---------|-----------|-------------|
| Vendas 2023 (completo) | Parquet | ~1.000.000 | `datasets/aula_02/vendas_2023_completo.parquet` |
| Clientes | Parquet | ~500.000 | `datasets/aula_02/clientes.parquet` |
| Categorias | JSON | 10 | `datasets/aula_02/categorias.json` |

