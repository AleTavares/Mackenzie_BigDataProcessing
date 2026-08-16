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
| 1 | Joins com múltiplas fontes de dados | `01_joins_multifonte.md` | 30 min |
| 2 | Window Functions: ranking e tendências | `02_window_functions.md` | 30 min |
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

---

## Validação de Tempo

| Bloco | Limite | Alocado | Status |
|-------|--------|---------|--------|
| Lab Parte 1 (guiado) | 60 min | 60 min | ✅ Dentro do limite |
| Lab Parte 2 (intermediário + desafio) | 50 min | 50 min | ✅ Dentro do limite |
| **Total** | **110 min** | **110 min** | ✅ **OK** |

---

## Notas para o Instrutor

- **Parte 1** é guiada com todos os comandos e resultados esperados. Os alunos devem acompanhar executando célula a célula.
- O **Exercício 1 (joins)** é o mais longo e fundamental — garanta que todos completem antes de avançar. Se necessário, sacrifique tempo do exercício 5 (desafio).
- No **Exercício 2 (window functions)**, o conceito de `partitionBy` vs `groupBy` costuma gerar dúvidas. Reforce a analogia: "groupBy colapsa linhas; window mantém todas as linhas".
- Na **Parte 2**, os exercícios são progressivamente mais abertos. Ofereça dicas verbais mas evite dar a solução completa.
- O **Exercício 5 (desafio)** é opcional para alunos mais rápidos. Se o tempo estiver apertado, pode ser atribuído como tarefa de casa.
- **Performance de UDFs:** o exercício 3 demonstra o impacto dramático — garanta que os alunos rodem o benchmark e vejam os números com seus próprios olhos.
- Consulte `06_troubleshooting.md` para erros comuns (OOM em joins, broadcast threshold, cache não materializado).
- Os datasets de 1M registros podem demorar ~10-15s para joins no ambiente Docker local — isso é esperado e serve como motivação para as otimizações.
- Reserve os últimos 2-3 min de cada parte para dúvidas rápidas antes do intervalo.
