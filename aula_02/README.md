# Aula 2 — Transformações Avançadas com Spark

**Disciplina:** Big Data Processing — MBA Engenharia de Dados (Mackenzie)

---

## Contexto Narrativo

> **"Black Friday se aproxima."**
>
> A DataFlow cresceu 10x. Ana (Product Owner) precisa de relatórios que cruzam vendas (1M registros) com clientes (500K) e categorias de produtos para a estratégia de Black Friday. Carlos tenta com pandas: `MemoryError: Unable to allocate 3.7 GiB for merge operation`. Marina (CTO) define a solução: joins distribuídos do Spark e Window Functions para rankings.

---

## Objetivos de Aprendizagem

Ao final desta aula, o aluno será capaz de:

1. **Dominar** todos os tipos de join no Spark (inner, left, right, full, cross, semi, anti)
2. **Aplicar** Broadcast Joins para otimizar joins com tabelas pequenas
3. **Utilizar** Window Functions para rankings, tendências e totais acumulados
4. **Criar** UDFs (User Defined Functions) e entender seu impacto na performance
5. **Analisar** planos de execução com `explain()` e identificar gargalos
6. **Otimizar** queries com particionamento, cache e persist

---

## Estrutura da Aula (4 horas)

| Bloco | Conteúdo | Duração |
|-------|----------|---------|
| Teoria | Slides HTML — Joins, Window Functions, UDFs, Catalyst | 50 min |
| Intervalo | — | 10 min |
| Lab Parte 1 | Setup + Joins multi-fonte + Window Functions (guiado) | 60 min |
| Intervalo | — | 10 min |
| Lab Parte 2 | UDFs + Análise de plano + Desafio de otimização | 50 min |

---

## Estrutura de Arquivos

```
aula_02/
├── README.md                  # Este arquivo
├── aula_02_slides.html        # Slides da teoria (HTML interativo)
├── code/
│   └── aula02_lab.ipynb       # Notebook do laboratório
├── data/
│   └── .gitkeep               # Dados montados via Docker
└── lab/
    ├── README.md              # Visão geral do lab (time budget, índice)
    ├── 00_setup.md            # Configuração e verificação dos datasets
    ├── 01_joins_multifonte.md # Exercício: joins (inner, left, broadcast, anti)
    ├── 02_window_functions.md # Exercício: ranking, lag/lead, running totals
    ├── 03_udfs_explain.md     # Exercício: UDFs, Pandas UDFs, performance
    ├── 04_analise_plano_execucao.md  # Exercício: leitura de planos de execução
    ├── 05_desafio_otimizacao.md      # Desafio: otimizar com broadcast+cache
    ├── 06_troubleshooting.md  # Guia de resolução de problemas
    └── ENTREGAVEL.md          # Informações sobre entregas
```

---

## Datasets

| Arquivo | Formato | Registros | Descrição |
|---------|---------|-----------|-----------|
| `vendas_2023_completo.parquet` | Parquet | ~1.000.000 | Vendas completas com customer_id e product_id |
| `clientes.parquet` | Parquet | ~500.000 | Cadastro de clientes (nome, segmento, estado) |
| `categorias.json` | JSON | 10 | Categorias de produtos (tabela de referência) |

Localização: `datasets/aula_02/`

---

## Tópicos Abordados

- Joins distribuídos: inner, left, right, full, cross, left_semi, left_anti
- Broadcast Join (otimização para tabelas pequenas)
- Window Functions: row_number, rank, dense_rank, lag, lead, ntile, percent_rank
- Running totals e médias móveis (rowsBetween, rangeBetween)
- UDFs vs Pandas UDFs vs funções built-in (comparativo de performance)
- Catalyst Optimizer: predicate pushdown, column pruning, join reordering
- Plano de execução: `explain()` (simple, extended, formatted)
- Estratégias de join internas (BroadcastHashJoin, SortMergeJoin, ShuffleHashJoin)
- Particionamento: `repartition()` vs `coalesce()`
- Cache e Persist (níveis de storage, quando usar)
- Data Skew e soluções (salting, AQE)

---

## Como Usar

1. **Teoria:** Abra `aula_02_slides.html` no navegador (navegação por setas do teclado)
2. **Lab:** Siga os exercícios em `lab/` na ordem (00 → 05)
3. **Notebook:** Use `code/aula02_lab.ipynb` como referência ou para execução direta

---

## Rodar no Google Colab (recomendado)

Se não quiser configurar Docker local, use o Google Colab:

1. Acesse [colab.research.google.com](https://colab.research.google.com)
2. Menu **Arquivo → Abrir notebook → GitHub**
3. Cole a URL do repositório: `https://github.com/AleTavares/Mackenzie_BigDataProcessing`
4. Selecione o notebook `aula_02/code/aula02_lab.ipynb`
5. Adicione esta célula no topo antes de rodar:

```python
# === SETUP COLAB ===
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!pip install pyspark -q
!git clone https://github.com/AleTavares/Mackenzie_BigDataProcessing.git /content/repo 2>/dev/null
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# Path dos datasets (ajuste para Colab)
DATA_PATH = "/content/repo/datasets/aula_02"
```

> **Nota:** No Colab, substitua os paths `/home/jovyan/work/data/aula_02/` por `/content/repo/datasets/aula_02/` nas células de leitura.

---

## Pré-requisitos

- Ter completado a Aula 1 (conceitos de Spark, SparkSession, DataFrame API)
- Docker rodando com pelo menos 8 GB de RAM (recomendado 12 GB para joins pesados)
- Ambiente iniciado: `docker compose -f shared/docker-compose.yml up -d`

---

## Navegação

⬅️ [Aula 1 — Fundamentos de Big Data e Apache Spark](../aula_01/) · ➡️ Aula 3 — Ingestão e Persistência de Dados
