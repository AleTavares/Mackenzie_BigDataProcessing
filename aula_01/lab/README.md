# Lab Aula 1 — Fundamentos de Big Data e Apache Spark

## Visão Geral

Laboratório hands-on da Aula 1, onde os alunos configuram o ambiente Spark com Docker e realizam suas primeiras análises de dados de vendas da DataFlow Analytics usando PySpark.

### Objetivos de Aprendizagem

- Configurar ambiente Apache Spark com Docker Compose
- Criar SparkSession e ler dados CSV
- Executar operações básicas com DataFrame API (select, filter, show)
- Realizar agregações com groupBy, sum, avg, count
- Comparar performance entre pandas e Spark

---

## Orçamento de Tempo (Time Budget)

| # | Exercício | Arquivo | Duração |
|---|-----------|---------|---------|
| — | **Lab Parte 1 — Guiado (60 min)** | | |
| 0 | Setup do ambiente Docker + Jupyter | `00_setup.md` | 10 min |
| 1 | Spark Básico: SparkSession, leitura, operações | `01_spark_basico.md` | 30 min |
| 2 | Agregações: groupBy, agg, orderBy | `02_agregacoes.md` | 20 min |
| | **Subtotal Parte 1** | | **60 min** |
| — | **Lab Parte 2 — Intermediário + Desafio (50 min)** | | |
| 3 | Análise exploratória de faturamento | `03_analise_exploratoria.md` | 25 min |
| 4 | Desafio: pandas vs Spark (timing) | `04_desafio_pandas_vs_spark.md` | 25 min |
| | **Subtotal Parte 2** | | **50 min** |
| | | | |
| | **Total Hands-on** | | **110 min** |

---

## Índice de Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `00_setup.md` | Configuração do ambiente Docker + Jupyter |
| `01_spark_basico.md` | Exercício guiado: SparkSession, leitura CSV, operações básicas |
| `02_agregacoes.md` | Exercício guiado: groupBy, agregações, ordenação |
| `03_analise_exploratoria.md` | Exercício intermediário: análise de faturamento por estado/cidade |
| `04_desafio_pandas_vs_spark.md` | Exercício desafio: comparação de performance pandas vs Spark |
| `05_troubleshooting.md` | Guia de resolução de problemas comuns |
| `ENTREGAVEL.md` | Especificação do entregável da aula |

---

## Validação de Tempo

| Bloco | Limite | Alocado | Status |
|-------|--------|---------|--------|
| Lab Parte 1 (guiado) | 60 min | 60 min | ✅ Dentro do limite |
| Lab Parte 2 (intermediário + desafio) | 50 min | 50 min | ✅ Dentro do limite |
| **Total** | **110 min** | **110 min** | ✅ **OK** |

---

## Notas para o Instrutor

- **Parte 1** é inteiramente guiada — todos os comandos e resultados esperados estão detalhados. Circule pela sala para garantir que ninguém trave no setup.
- O **setup (10 min)** assume que o Docker já está instalado. Se houver alunos sem Docker, direcione-os ao GitHub Codespaces.
- Na **Parte 2**, os exercícios são progressivamente mais abertos. Ofereça dicas verbais mas evite dar a solução completa.
- O **Exercício 4 (desafio)** é opcional para alunos mais rápidos. Se o tempo estiver apertado, pode ser atribuído como tarefa de casa.
- Consulte `05_troubleshooting.md` para erros comuns que os alunos podem encontrar (Spark não conecta, OOM, etc.).
- Reserve os últimos 2-3 min de cada parte para dúvidas rápidas antes do intervalo.
