# Aula 1 — Fundamentos de Big Data e Apache Spark

**Disciplina:** Big Data Processing — MBA Engenharia de Dados (Mackenzie)

---

## Contexto Narrativo

> **"O script morreu."**
>
> Carlos Mendes (Engenheiro de Dados Sênior) chega ao escritório numa segunda-feira e encontra o temido `Killed` no terminal. O relatório de vendas da ShopBrasil — 100 mil registros — matou o script pandas após 4h37min. Marina (CTO) decide: é hora de migrar para processamento distribuído.

---

## Objetivos de Aprendizagem

Ao final desta aula, o aluno será capaz de:

1. **Compreender** o conceito de Big Data e os 5 V's
2. **Explicar** por que pandas tem limite de escala
3. **Descrever** a arquitetura do Spark (Driver, Executors, Cluster Manager)
4. **Diferenciar** transformações de ações (lazy evaluation)
5. **Conhecer** o ecossistema Spark e o papel do Catalyst Optimizer
6. **Executar** operações básicas com DataFrame API (select, filter, groupBy, agg)

---

## Estrutura da Aula (4 horas)

| Bloco | Conteúdo | Duração |
|-------|----------|---------|
| Teoria | Slides HTML — Big Data, Spark, Lazy Evaluation, Catalyst | 50 min |
| Intervalo | — | 10 min |
| Lab Parte 1 | Setup + Spark Básico + Agregações (guiado) | 60 min |
| Intervalo | — | 10 min |
| Lab Parte 2 | Análise exploratória + Desafio pandas vs Spark | 50 min |

---

## Estrutura de Arquivos

```
aula_01/
├── README.md                  # Este arquivo
├── aula_01_slides.html        # Slides da teoria (HTML interativo)
├── code/
│   └── aula01_lab.ipynb       # Notebook do laboratório
├── data/
│   └── .gitkeep               # Dados montados via Docker
└── lab/
    ├── README.md              # Visão geral do lab (time budget, índice)
    ├── 00_setup.md            # Configuração do ambiente Docker
    ├── 01_spark_basico.md     # Exercício: SparkSession, leitura, operações
    ├── 02_agregacoes.md       # Exercício: groupBy, agg, orderBy
    ├── 03_analise_exploratoria.md  # Exercício: faturamento por estado/cidade
    ├── 04_desafio_pandas_vs_spark.md  # Desafio: benchmark pandas vs Spark
    ├── 05_troubleshooting.md  # Guia de resolução de problemas
    └── ENTREGAVEL.md          # Informações sobre entregas
```

---

## Datasets

| Arquivo | Formato | Registros | Descrição |
|---------|---------|-----------|-----------|
| `vendas_2023.csv` | CSV | ~100.000 | Vendas da ShopBrasil (order_id, total_amount, shipping_state, etc.) |
| `produtos.csv` | CSV | ~5.000 | Catálogo de produtos |

Localização: `datasets/aula_01/`

---

## Tópicos Abordados

- Big Data e os 5 V's (Volume, Velocidade, Variedade, Veracidade, Valor)
- Limitações do pandas (memória, single-thread)
- Escala Vertical vs Horizontal
- Arquitetura do Apache Spark (Driver, Executors, Cluster Manager)
- Lazy Evaluation e DAG (Directed Acyclic Graph)
- Transformações vs Ações
- DataFrame API: `select()`, `filter()`, `groupBy()`, `agg()`, `orderBy()`
- Catalyst Optimizer e Tungsten
- Ecossistema Spark (SQL, MLlib, Streaming, GraphX)
- Boas práticas iniciais (schema explícito, Parquet, evitar collect())

---

## Como Usar

1. **Teoria:** Abra `aula_01_slides.html` no navegador (navegação por setas do teclado)
2. **Lab:** Siga os exercícios em `lab/` na ordem (00 → 04)
3. **Notebook:** Use `code/aula01_lab.ipynb` como referência ou para execução direta

---

## Rodar no Google Colab (recomendado)

Se não quiser configurar Docker local, use o Google Colab:

1. Acesse [colab.research.google.com](https://colab.research.google.com)
2. Menu **Arquivo → Abrir notebook → GitHub**
3. Cole a URL do repositório: `https://github.com/AleTavares/Mackenzie_BigDataProcessing`
4. Selecione o notebook `aula_01/code/aula01_lab.ipynb`
5. Adicione esta célula no topo antes de rodar:

```python
# === SETUP COLAB ===
!apt-get install openjdk-17-jdk-headless -qq > /dev/null
!pip install pyspark -q
!git clone https://github.com/AleTavares/Mackenzie_BigDataProcessing.git /content/repo 2>/dev/null
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# Path dos datasets (ajuste para Colab)
DATA_PATH = "/content/repo/datasets/aula_01"
```

> **Nota:** No Colab, substitua os paths `/home/jovyan/work/data/aula_01/` por `/content/repo/datasets/aula_01/` nas células de leitura.

---

## Pré-requisitos

- Docker rodando com pelo menos 8 GB de RAM alocados
- Familiaridade básica com Python e pandas
- Ter executado `docker compose -f shared/docker-compose.yml up -d`

---

## Próxima Aula

➡️ [Aula 2 — Transformações Avançadas com Spark](../aula_02/)
