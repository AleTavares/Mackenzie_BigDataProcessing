# Exercício 4 — Desafio: Pandas vs Spark — Benchmark de Performance

## Contexto

> **Marina Silva (CTO):** "Carlos, eu sei que Spark é o caminho certo para a DataFlow, mas o Roberto me pediu números concretos. Ele quer ver uma comparação lado a lado: quanto tempo o pandas leva versus o Spark nas mesmas operações. Sem isso, ele não vai aprovar o investimento em infraestrutura distribuída. Preciso de um benchmark real, não de teoria."

> **Carlos Mendes (Eng. Sênior):** "Faz sentido, Marina. Vou montar um benchmark comparativo usando nosso dataset de vendas. Vou medir leitura, agregação e filtragem nos dois frameworks. Mas aviso: com 100K registros, o pandas ainda vai se sair bem — o Spark brilha mesmo é com milhões de registros. Vou projetar os tempos para volumes maiores também."

## Objetivos

Ao final deste exercício, você será capaz de:

- Medir e comparar tempos de execução entre pandas e PySpark
- Entender o overhead do Spark para datasets pequenos (JVM startup, serialização)
- Identificar o ponto de crossover onde Spark supera o pandas
- Projetar comportamento de performance para datasets em escala real
- Formular uma recomendação técnica baseada em dados

## Pré-requisitos

- Exercícios 1, 2 e 3 concluídos
- Ambiente Docker rodando com Spark + Jupyter (ver `00_setup.md`)
- Dataset `vendas_2023.csv` disponível na pasta `data/`
- Bibliotecas disponíveis: `pandas`, `pyspark`, `time`

## Duração Estimada

⏱️ ~25 minutos (parte final opcional do Lab Parte 2 — 50 min total)

## Nível de Dificuldade

🔴 **Desafio** — Orientação mínima. Você recebe as perguntas e o boilerplate de timing, mas deve construir toda a solução de forma independente. Este exercício é opcional e voltado para quem quer ir além.

---

## Setup Inicial

Use este boilerplate para medir tempos de execução em todas as comparações:

```python
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

# SparkSession (já deve estar criada dos exercícios anteriores)
spark = SparkSession.builder \
    .appName("DataFlow-Benchmark") \
    .master("local[*]") \
    .getOrCreate()

# Caminho do dataset
DATASET_PATH = "data/vendas_2023.csv"

# Função auxiliar para medir tempo
def medir_tempo(descricao, funcao):
    """Executa uma função e retorna o tempo em segundos."""
    inicio = time.time()
    resultado = funcao()
    fim = time.time()
    tempo = fim - inicio
    print(f"⏱️ {descricao}: {tempo:.4f} segundos")
    return tempo, resultado
```

---

## Exercício 4.1: Comparação de Leitura (Load)

### Pergunta de Negócio

> **Roberto Tanaka (CEO):** "Quanto tempo leva pra carregar nossos dados de vendas? Se o Spark demora mais só pra abrir o arquivo, por que eu pagaria mais por ele?"

### O que fazer

Meça o tempo de leitura do mesmo arquivo `vendas_2023.csv` usando:
1. **pandas**: `pd.read_csv()`
2. **PySpark**: `spark.read.csv()` + `.count()` (force a materialização com `count()` — lembre-se do lazy evaluation!)

### Boilerplate de Início

```python
# --- PANDAS ---
tempo_pandas_load, df_pandas = medir_tempo(
    "pandas read_csv",
    lambda: pd.read_csv(DATASET_PATH)
)

# --- SPARK ---
def spark_load():
    df = spark.read.csv(DATASET_PATH, header=True, inferSchema=True)
    df.count()  # Força materialização (lazy evaluation)
    return df

tempo_spark_load, df_spark = medir_tempo(
    "Spark read.csv + count",
    spark_load
)

# Comparação
print(f"\n📊 Resultado Load:")
print(f"   pandas: {tempo_pandas_load:.4f}s")
print(f"   Spark:  {tempo_spark_load:.4f}s")
print(f"   Ratio:  Spark é {tempo_spark_load/tempo_pandas_load:.1f}x {'mais lento' if tempo_spark_load > tempo_pandas_load else 'mais rápido'}")
```

### Perguntas para Reflexão

- Por que o Spark provavelmente é mais lento nesta operação com 100K registros?
- O que é o overhead da JVM e como ele impacta operações em datasets pequenos?
- O que acontece se você rodar a leitura do Spark uma segunda vez (sem recriar a SparkSession)?

---

## Exercício 4.2: Comparação de Agregação (groupBy)

### Pergunta de Negócio

> **Marina:** "OK, entendi o custo de startup. Mas e para análises reais? Quando a gente agrupa faturamento por estado — que é o que fazemos todo dia — qual framework é mais rápido?"

### O que fazer

Execute a **mesma operação** em ambos os frameworks:
- Agrupar por `shipping_state`
- Calcular: soma de `total_amount` (faturamento), contagem de pedidos, média de `total_amount`
- Ordenar por faturamento descendente

Meça o tempo de cada operação.

### Dicas

- No pandas: use `df.groupby("shipping_state").agg(...)` com `reset_index()` e `sort_values()`
- No Spark: use `df.groupBy("shipping_state").agg(...)` com `orderBy()`
- No Spark, force a materialização com `.collect()` ou `.show()` para medir o tempo real (não apenas a construção do plano)

### Perguntas para Reflexão

- A diferença de tempo é maior ou menor que na leitura? Por quê?
- O Catalyst Optimizer do Spark adiciona algum benefício nesta operação?
- Em qual volume de dados você espera que o Spark comece a vencer?

---

## Exercício 4.3: Comparação de Filtro Complexo

### Pergunta de Negócio

> **Ana:** "Marina, preciso de um relatório filtrado: apenas pedidos entregues, acima de R$500, nos estados do Sudeste (SP, RJ, MG, ES). Qual engine responde isso mais rápido?"

### O que fazer

Aplique o **mesmo filtro complexo** em ambos os frameworks:
- `shipping_state` IN ('SP', 'RJ', 'MG', 'ES')
- `total_amount` > 500
- `status` == 'delivered'

Meça o tempo de aplicação do filtro + contagem dos resultados.

### Dicas

- No pandas: combine condições com `&` e indexação booleana: `df[(cond1) & (cond2) & (cond3)]`
- No Spark: use `.filter()` com condições encadeadas ou expressão SQL com `.where()`
- Inclua `.count()` no final de ambos para garantir que a operação foi executada completamente

### Perguntas para Reflexão

- Filtros beneficiam mais o Spark ou o pandas em dados pequenos?
- O que é "predicate pushdown" e como ele ajudaria com dados em Parquet?
- Se o dataset estivesse particionado por `shipping_state`, como isso mudaria o resultado?

---

## Exercício 4.4: Projeção de Escala

### Pergunta de Negócio

> **Roberto:** "Tudo bem, com 100 mil registros o pandas aguenta. Mas a gente vai crescer. Quando chegarmos a 1 milhão, 10 milhões, 100 milhões de registros — o que acontece? Me mostra uma projeção."

### O que fazer

Com base nos tempos observados nos exercícios 4.1 a 4.3, projete os tempos para volumes maiores. Considere:
- **pandas**: escala de forma aproximadamente linear (O(n)) com uso crescente de memória
- **Spark**: overhead fixo alto (~2-5s de startup), mas escala de forma sublinear em cluster (O(n/p) onde p = partições/cores)

Preencha a tabela abaixo com suas projeções:

### Formato da Tabela de Projeção

```
| Registros | pandas (Load) | Spark (Load) | pandas (groupBy) | Spark (groupBy) | Vencedor |
|-----------|---------------|--------------|------------------|-----------------|----------|
| 100K      | (medido) s    | (medido) s   | (medido) s       | (medido) s      | pandas   |
| 1M        | (projeção) s  | (projeção) s | (projeção) s     | (projeção) s    | ???      |
| 10M       | (projeção) s  | (projeção) s | (projeção) s     | (projeção) s    | ???      |
| 100M      | (projeção) s  | (projeção) s | (projeção) s     | (projeção) s    | ???      |
| 1B        | OOM?          | (projeção) s | OOM?             | (projeção) s    | Spark    |
```

### Dicas

- Para pandas: multiplique o tempo proporcionalmente ao volume. Considere que acima de ~5-10M registros, a memória RAM (8GB) pode não ser suficiente (OOM = Out of Memory)
- Para Spark: o overhead fixo (~2-5s) se mantém constante, mas o tempo de processamento escala de forma mais suave graças ao paralelismo
- Em um cluster real (não `local[*]`), o Spark escalaria ainda melhor ao adicionar workers
- Use `matplotlib` ou simplesmente print formatado para apresentar a projeção

---

## Exercício 4.5: Análise e Conclusão

### Pergunta de Negócio

> **Marina:** "Roberto, aqui estão os números. Baseado nos benchmarks e projeções, minha recomendação técnica é..."

### O que fazer

Escreva uma conclusão técnica (3-5 parágrafos em uma célula Markdown no notebook) respondendo:

1. **Quando o pandas é melhor?**
   - Para qual faixa de volume de dados?
   - Quais tipos de operação?
   - Em quais cenários de uso (exploração ad-hoc, scripts pontuais)?

2. **Quando o Spark vence?**
   - A partir de qual volume o overhead se paga?
   - Quais operações se beneficiam mais do paralelismo?
   - Quais cenários justificam o investimento (pipelines diários, processamento recorrente)?

3. **Qual é o ponto de crossover?**
   - Com base nos seus benchmarks, em qual volume (aproximado) o Spark começa a ser mais rápido?
   - Esse ponto muda dependendo da operação (load vs groupBy vs filter)?

4. **Recomendação para a DataFlow:**
   - Com 100K registros hoje e projeção de crescimento 10x em 6 meses, o investimento em Spark se justifica?

### Fatores que Afetam a Performance (para incluir na análise)

| Fator | Impacto no pandas | Impacto no Spark |
|-------|-------------------|------------------|
| JVM Startup | N/A | +2-5s overhead fixo |
| Serialização Python ↔ JVM | N/A | Custo em UDFs e collect() |
| Overhead pequenos datasets | Mínimo | Significativo |
| Paralelismo | Limitado (1 core) | Escala com cores/workers |
| Memória | Limitado à RAM | Spill to disk |
| Lazy Evaluation | N/A | Otimização do plano |
| Formato de arquivo | Pouco impacto | Predicate pushdown, partition pruning |

---

## Entregável

Ao final deste desafio, seu notebook deve conter:

- [ ] Benchmarks reais (medidos) para os 3 tipos de operação
- [ ] Tabela de projeção de escala preenchida com suas estimativas
- [ ] Conclusão escrita respondendo as 4 perguntas acima
- [ ] Recomendação final para a DataFlow Analytics

---

## Dica Final

> **Carlos:** "Lembre-se: este benchmark com `local[*]` está rodando o Spark em uma única máquina. Em um cluster real com 10+ workers, a vantagem do Spark para datasets grandes seria ainda mais dramática. O pandas nunca vai escalar horizontalmente — essa é a diferença fundamental."

---

## Próxima Aula

➡️ **Aula 2 — Transformações Avançadas com Spark**: Joins, Window Functions, UDFs e otimização de planos de execução. A DataFlow cresceu 10x e agora precisa cruzar dados de múltiplas fontes!
