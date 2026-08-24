# Aula 3 — Ingestão e Persistência de Dados

**Disciplina:** Big Data Processing — MBA Engenharia de Dados (Mackenzie)

---

## Contexto Narrativo

> **"Três parceiros, três formatos."**
>
> A DataFlow fechou com 3 novos fornecedores de dados. Cada parceiro envia em formato diferente: CSV legado com encoding ISO-8859-1, JSON de API paginada e Parquet otimizado. Marina (CTO) define a Arquitetura Medallion para organizar o data lake: Bronze (raw), Silver (normalizado) e Gold (agregado).

---

## Objetivos de Aprendizagem

Ao final desta aula, o aluno será capaz de:

1. **Implementar** ingestão de múltiplos formatos (CSV, JSON, Parquet, Delta)
2. **Comparar** trade-offs entre formatos de arquivo para big data
3. **Aplicar** schema evolution e schema enforcement em pipelines
4. **Particionar** dados para otimizar leitura e escrita
5. **Implementar** a arquitetura Medallion (Bronze → Silver → Gold)
6. **Escolher** o modo de escrita correto (append, overwrite, merge)

---

## Estrutura da Aula (4 horas)

| Bloco | Conteúdo | Duração |
|-------|----------|---------|
| Teoria | Slides HTML — Formatos, Medallion, Particionamento, Schema | 50 min |
| Intervalo | — | 10 min |
| Lab Parte 1 | Ingestão multi-formato + Camada Bronze (guiado) | 60 min |
| Intervalo | — | 10 min |
| Lab Parte 2 | Camadas Silver e Gold + Desafio | 50 min |

---

## Estrutura de Arquivos

```
aula_03/
├── README.md                  # Este arquivo
├── aula_03_slides.html        # Slides da teoria (HTML interativo)
├── code/
│   └── aula03_lab.ipynb       # Notebook do laboratório
├── data/
│   └── .gitkeep               # Dados montados via Docker
└── lab/
    ├── README.md              # Visão geral do lab (time budget, índice)
    ├── 01_ingestao_csv_legado.md    # Exercício: CSV encoding ISO-8859-1
    ├── 02_ingestao_json_parquet.md  # Exercício: JSON nested + Parquet
    ├── 03_camada_bronze.md          # Exercício: Bronze com metadados
    ├── 04_camada_silver.md          # Exercício: Silver normalizado
    ├── 05_persistencia_particionada.md  # Exercício: particionamento
    ├── 06_camada_gold.md            # Exercício: Gold agregações
    ├── 07_troubleshooting.md        # Guia de resolução de problemas
    └── ENTREGAVEL.md                # Informações sobre entregas
```

---

## Datasets

| Arquivo | Formato | Descrição |
|---------|---------|-----------|
| `parceiro_a/vendas_legacy_*.csv` | CSV (ISO-8859-1, sep=`;`) | Vendas do ERP legado |
| `parceiro_b/api_dump_page_*.json` | JSON (nested, arrays) | Dump de API REST |
| `parceiro_c/vendas_*.parquet` | Parquet (Snappy) | Export do data lake do parceiro |

Localização: `datasets/aula_03/`

---

## Tópicos Abordados

- Formatos de arquivo: CSV, JSON, Parquet, ORC, Avro, Delta Lake
- Tabela comparativa (compressão, column pruning, schema, splittability)
- Codecs de compressão: Snappy, GZIP, ZSTD, LZ4
- Arquitetura Medallion: Bronze → Silver → Gold
- Particionamento de dados e partition pruning
- Small files problem e over-partitioning
- Schema evolution vs Schema enforcement
- Modos de escrita: append, overwrite, ignore, error
- MERGE (upsert) com Delta Lake
- Anatomia de um arquivo Parquet (row groups, column chunks, footer)

---

## Como Usar

1. **Teoria:** Abra `aula_03_slides.html` no navegador (navegação por setas do teclado)
2. **Lab:** Siga os exercícios em `lab/` na ordem (01 → 06)
3. **Notebook:** Use `code/aula03_lab.ipynb` como referência ou para execução direta

---

## Rodar no Google Colab (recomendado)

Se não quiser configurar Docker local, use o Google Colab:

1. Acesse [colab.research.google.com](https://colab.research.google.com)
2. Menu **Arquivo → Abrir notebook → GitHub**
3. Cole a URL do repositório: `https://github.com/AleTavares/Mackenzie_BigDataProcessing`
4. Selecione o notebook `aula_03/code/aula03_lab.ipynb`
5. Adicione esta célula no topo antes de rodar:

```python
# === SETUP COLAB ===
!apt-get install openjdk-17-jdk-headless -qq > /dev/null
!pip install pyspark -q
!git clone https://github.com/AleTavares/Mackenzie_BigDataProcessing.git /content/repo 2>/dev/null
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# Path dos datasets (ajuste para Colab)
DATA_PATH = "/content/repo/datasets/aula_03"
```

> **Nota:** No Colab, substitua os paths `/home/jovyan/work/data/aula_03/` por `/content/repo/datasets/aula_03/` nas células de leitura.

---

## Pré-requisitos

- Ter completado as Aulas 1 e 2 (Spark, DataFrame API, joins)
- Docker rodando com pelo menos 8 GB de RAM
- Ambiente iniciado: `docker compose -f shared/docker-compose.yml up -d`

---

## Navegação

⬅️ [Aula 2 — Transformações Avançadas](../aula_02/) · ➡️ [Aula 4 — Introdução ao Apache Airflow](../aula_04/)
