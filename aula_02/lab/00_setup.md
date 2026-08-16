# Lab Setup - Aula 2: Transformações Avançadas com Spark

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Hoje vamos trabalhar com 3 fontes de dados diferentes — vendas em Parquet (1M registros), clientes em Parquet (500K registros) e categorias em JSON. O ambiente é o mesmo da Aula 1, mas vamos confirmar que tudo está rodando e que os novos datasets estão acessíveis."

## Pré-requisitos

Antes de iniciar, verifique que você possui os seguintes itens instalados e configurados:

| Requisito | Versão Mínima | Como Verificar |
|-----------|---------------|----------------|
| Docker Desktop (Windows/Mac) ou Docker Engine (Linux) | 24.0+ | `docker --version` |
| Docker Compose v2 | 2.20+ | `docker compose version` |
| Git | 2.30+ | `git --version` |
| RAM disponível | 8 GB | Docker Desktop → Settings → Resources |
| CPU cores | 4 cores | Docker Desktop → Settings → Resources |

> **⚠️ Importante:** Os datasets desta aula são maiores (1M + 500K registros). Garanta pelo menos **8 GB de RAM** alocados para o Docker. Se possível, use **12 GB** para evitar problemas com cache e joins pesados.

---

## Passo 1: Atualizar o Repositório

**Descrição:** Garantir que você tem a versão mais recente do repositório com os datasets da Aula 2.

**Comando:**
```bash
cd Mackenzie_BigDataProcessing
git pull origin main
```

**Resultado esperado:**
```
Already up to date.
```
ou
```
Updating f4d9f8a..be3c856
Fast-forward
 aula_02/... | ...
```

**Explicação:** Os datasets da Aula 2 (Parquet e JSON) foram adicionados ao repositório. Se você já clonou na Aula 1, basta atualizar com `git pull`.

---

## Passo 2: Subir o Ambiente com Docker Compose

**Descrição:** Iniciar o container do Jupyter Notebook com PySpark. Se o ambiente já estiver rodando da Aula 1, pule direto para o Passo 3.

**Comando (opção 1 — Docker Compose direto):**
```bash
docker compose -f shared/docker-compose.yml up -d
```

**Comando (opção 2 — script auxiliar):**
```bash
./shared/start_env.sh base
```

**Resultado esperado:**
```
[+] Running 2/2
 ✔ Network shared_default      Created
 ✔ Container jupyter-spark     Started
```

**Explicação:** O ambiente é o mesmo da Aula 1 — Jupyter com PySpark em modo local (`local[*]`). Os datasets da Aula 2 são montados automaticamente na pasta `data/aula_02/` dentro do container.

**Dica:** Se o container já estava rodando, o comando mostrará "Container jupyter-spark Running" — isso é normal.

---

## Passo 3: Verificar os Datasets da Aula 2

**Descrição:** Confirmar que os 3 datasets estão acessíveis no Jupyter.

**Comando (no Jupyter — crie um notebook novo ou use o terminal):**

```python
import os

base_path = "/home/jovyan/work/data/aula_02"

arquivos_esperados = [
    "vendas_2023_completo.parquet",
    "clientes.parquet",
    "categorias.json"
]

print("📂 Verificando datasets da Aula 2:")
print(f"   Diretório: {base_path}")
print()

for arquivo in arquivos_esperados:
    caminho = os.path.join(base_path, arquivo)
    if os.path.exists(caminho):
        tamanho = os.path.getsize(caminho) / (1024 * 1024)  # MB
        print(f"   ✅ {arquivo} ({tamanho:.1f} MB)")
    else:
        print(f"   ❌ {arquivo} — NÃO ENCONTRADO!")

print()
print("Se algum arquivo estiver faltando, execute: git pull origin main")
```

**Resultado esperado:**
```
📂 Verificando datasets da Aula 2:
   Diretório: /home/jovyan/work/data/aula_02

   ✅ vendas_2023_completo.parquet (~XX.X MB)
   ✅ clientes.parquet (~XX.X MB)
   ✅ categorias.json (~0.0 MB)
```

**Explicação:** Os 3 datasets representam o cenário da DataFlow Analytics:
- **vendas_2023_completo.parquet** — 1M de registros de vendas (formato colunar otimizado)
- **clientes.parquet** — 500K clientes cadastrados (dados de dimensão)
- **categorias.json** — 10 categorias de produtos (tabela de referência pequena)

---

## Passo 4: Testar SparkSession e Leitura Multi-Formato

**Descrição:** Verificar que o Spark consegue ler os 3 formatos de arquivo que usaremos no lab.

**Comando (no Jupyter):**

```python
from pyspark.sql import SparkSession

# Criar SparkSession com configuração otimizada para a Aula 2
spark = SparkSession.builder \
    .appName("DataFlow-Aula02-Setup") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10m") \
    .getOrCreate()

print(f"✅ SparkSession criada!")
print(f"   Versão: {spark.version}")
print(f"   Master: {spark.sparkContext.master}")
print(f"   Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print()

# Testar leitura dos 3 formatos
base = "/home/jovyan/work/data/aula_02"

df_vendas = spark.read.parquet(f"{base}/vendas_2023_completo.parquet")
df_clientes = spark.read.parquet(f"{base}/clientes.parquet")
df_categorias = spark.read.json(f"{base}/categorias.json", multiLine=True)

print(f"📊 Vendas:     {df_vendas.count():>10,} registros | {len(df_vendas.columns)} colunas")
print(f"📊 Clientes:   {df_clientes.count():>10,} registros | {len(df_clientes.columns)} colunas")
print(f"📊 Categorias: {df_categorias.count():>10,} registros | {len(df_categorias.columns)} colunas")
print()
print("✅ Todos os datasets carregados com sucesso!")

# Encerrar sessão de teste
spark.stop()
```

**Resultado esperado:**
```
✅ SparkSession criada!
   Versão: 3.5.x
   Master: local[*]
   Shuffle Partitions: 8

📊 Vendas:      1,000,000 registros | 12 colunas
📊 Clientes:      500,000 registros | 8 colunas
📊 Categorias:          1 registros | 1 colunas

✅ Todos os datasets carregados com sucesso!
```

**Explicação:**
- **`spark.sql.shuffle.partitions = 8`** — reduzimos de 200 (default) para 8 porque estamos em ambiente local. Valor alto demais em dados pequenos gera overhead desnecessário
- **`spark.sql.autoBroadcastJoinThreshold = 10m`** — tabelas menores que 10MB serão automaticamente "broadcast" (enviadas a todos os executores), evitando shuffle
- **`spark.driver.memory = 2g`** — alocamos 2GB para o driver, suficiente para os joins da aula
- A leitura de Parquet é significativamente mais rápida que CSV — formato colunar com compressão embutida

**Dica:** Se a contagem de vendas demorar mais de 30 segundos, verifique se o Docker tem RAM suficiente alocada (mínimo 8GB).

---

## Passo 5: Verificar Spark App UI

**Descrição:** Após executar o código do Passo 4, a Spark App UI deve estar acessível. Na Aula 2, usaremos essa interface para analisar planos de execução e monitorar joins.

**Comando:**
```
Abra o navegador: http://localhost:4040
```

**Resultado esperado:**
- Interface do Spark mostrando jobs executados (leituras e counts do Passo 4)
- Abas: Jobs, Stages, Storage, Environment, Executors, SQL

**Explicação:** A Spark App UI será fundamental nesta aula para:
- Visualizar planos de execução de joins (`explain()` mostra texto; a UI mostra graficamente)
- Monitorar volume de shuffle entre executors
- Verificar se broadcast join está sendo usado
- Acompanhar uso de cache/persist

> **Nota:** A UI só fica ativa enquanto um SparkSession está rodando. Se você executou `spark.stop()` no Passo 4, a UI fecha. Ela voltará quando iniciar o notebook do lab.

---

## Passo 6: Encerrar o Ambiente (Pós-Lab)

**Descrição:** Ao final do laboratório, parar o container para liberar recursos.

**Comando (opção 1 — Docker Compose direto):**
```bash
docker compose -f shared/docker-compose.yml down
```

**Comando (opção 2 — script auxiliar):**
```bash
./shared/stop_env.sh
```

**Resultado esperado:**
```
[+] Running 2/2
 ✔ Container jupyter-spark     Removed
 ✔ Network shared_default      Removed
```

**Explicação:** Os notebooks salvos ficam persistidos no volume montado. Na próxima aula, basta `docker compose up -d` novamente.

---

## Troubleshooting

### Problema: Parquet não encontrado ou erro de leitura

**Sintoma:** `AnalysisException: Path does not exist` ao tentar ler o Parquet.

**Solução:**
```bash
# Verificar se os datasets existem no host:
ls datasets/aula_02/

# Se estiver vazio, atualize o repositório:
git pull origin main

# Reinicie o container para remontar os volumes:
docker compose -f shared/docker-compose.yml restart
```

---

### Problema: OutOfMemoryError durante joins

**Sintoma:** `java.lang.OutOfMemoryError: Java heap space` ao executar joins com 1M registros.

**Solução:**
```python
# Opção 1: Aumentar memória do driver
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Opção 2: Reduzir partições de shuffle (menos overhead)
spark.conf.set("spark.sql.shuffle.partitions", "4")
```

Se persistir, aumente a RAM do Docker para 12GB em Settings → Resources.

---

### Problema: Join muito lento (> 2 minutos para 1M registros)

**Sintoma:** O join de vendas com clientes demora mais de 2 minutos no ambiente local.

**Solução:**
```python
# 1. Verificar número de partições (muitas partições = overhead)
print(f"Partições vendas: {df_vendas.rdd.getNumPartitions()}")
print(f"Partições clientes: {df_clientes.rdd.getNumPartitions()}")

# 2. Reduzir shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "4")

# 3. Se uma tabela é pequena, use broadcast:
from pyspark.sql.functions import broadcast
df_resultado = df_vendas.join(broadcast(df_pequena), "chave", "left")
```

---

### Problema: Cache não está acelerando (mesma performance)

**Sintoma:** Após `df.cache()`, as queries subsequentes não ficam mais rápidas.

**Solução:**
```python
# Cache é LAZY — precisa de uma ação para materializar:
df_completo.cache()
df_completo.count()  # ← ESTA LINHA materializa o cache!

# Agora as próximas ações usarão o cache:
df_completo.groupBy("estado").count().show()  # Rápido ⚡
```

---

### Problema: "Port already in use" / Container não sobe

Consulte o arquivo `aula_01/lab/00_setup.md` — as soluções de troubleshooting de Docker são as mesmas.

---

## Checklist de Validação

Antes de prosseguir para os exercícios do lab, confirme:

- [ ] Container `jupyter-spark` está "Up" (`docker compose ps`)
- [ ] Jupyter Notebook acessível em http://localhost:8888
- [ ] Arquivo `vendas_2023_completo.parquet` acessível (~1M registros)
- [ ] Arquivo `clientes.parquet` acessível (~500K registros)
- [ ] Arquivo `categorias.json` acessível (10 categorias)
- [ ] SparkSession cria e lê os 3 formatos com sucesso
- [ ] Spark App UI acessível em http://localhost:4040 (com SparkSession ativa)

> **Carlos:** "Ambiente confirmado! Temos 1 milhão de vendas, 500 mil clientes e 10 categorias prontos para cruzar. A Black Friday da Ana depende de nós — vamos para os joins!"
