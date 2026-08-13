# Revisão de Tempos de Execução — Labs Big Data Processing

> **Critérios de referência:**
> - Lab Parte 1 (guiado): 60 min | cada exercício < 15 min
> - Lab Parte 2 (intermediário + desafio): 50 min
> - Total do lab: ≤ 110 min

---

## 1. Tabela Resumo por Aula

| Aula | Parte 1 (Guiado) | Parte 2 (Interm. + Desafio) | Total | Status |
|------|------------------:|----------------------------:|------:|--------|
| Aula 01 | 60 min | 50 min | **110 min** | ✅ OK |
| Aula 02 | 60 min | 50 min | **110 min** | ✅ OK |
| Aula 03 | 60 min | 50 min | **110 min** | ✅ OK |
| Aula 04 | 75 min | 35 min | **110 min** | ⚠️ Parte 1 excede 60 min |
| Aula 05 | 55 min | 47 min | **102 min** | ✅ OK |
| Aula 06 | 60 min | 50 min | **110 min** | ✅ OK |
| Aula 07 | 50 min | 52 min | **102 min** | ✅ OK |

---

## 2. Análise Detalhada por Aula

---

### Aula 01 — Fundamentos de Big Data e Apache Spark

**Parte 1 — Guiado (60 min)**

| # | Exercício | Tempo | Passos | Complexidade | Notas |
|---|-----------|------:|--------|--------------|-------|
| 0 | Setup Docker + Jupyter | 10 min | 8 passos | Baixa | Espera por download de imagens na 1ª vez pode adicionar 5-10 min |
| 1 | Spark Básico: SparkSession, leitura, operações | 30 min | 9 passos | Baixa-Média | Muitos comandos mas todos guiados, inclui tempo de JVM startup |
| 2 | Agregações: groupBy, agg, orderBy | 20 min | 8 passos | Baixa-Média | Pipeline final combina múltiplas operações |

⚠️ **Exercício 1 (30 min):** Excede o limite de 15 min por exercício. Contudo, como inclui 9 passos progressivos (SparkSession, leitura CSV, schema, show, count, select, filter, where, encadeamento), o ritmo é constante e o conteúdo é coeso.

⚠️ **Exercício 2 (20 min):** Excede o limite de 15 min por exercício. Contém 8 passos com pipeline completo no final.

**Parte 2 — Intermediário + Desafio (50 min)**

| # | Exercício | Tempo | Nível | Notas |
|---|-----------|------:|-------|-------|
| 3 | Análise exploratória de faturamento | 25 min | 🟡 Intermediário | 6 sub-exercícios abertos, inclui Window Functions introdutórias |
| 4 | Desafio: pandas vs Spark (benchmark) | 25 min | 🔴 Desafio | Opcional; 5 sub-exercícios de benchmark + projeção + conclusão |

**Total: 110 min** ✅

---

### Aula 02 — Transformações Avançadas com Spark

**Parte 1 — Guiado (60 min)**

| # | Exercício | Tempo | Passos | Complexidade | Notas |
|---|-----------|------:|--------|--------------|-------|
| 1 | Joins multi-fonte (inner, left, broadcast, anti) | 30 min | 8 passos | Média | Lê 3 datasets (Parquet, JSON), 4 tipos de join, explain(), cache |
| 2 | Window Functions (ranking, lag, lead, churn) | 30 min | 8 passos | Média-Alta | Dense rank, variação percentual, análise de churn |

⚠️ **Exercício 1 (30 min):** Excede limite de 15 min. Justificado pela necessidade de setup (3 fontes), demonstração de 4 tipos de join, e explain(). Conteúdo é bastante denso.

⚠️ **Exercício 2 (30 min):** Excede limite de 15 min. Window Functions são conceito complexo que requer passos progressivos.

**Parte 2 — Intermediário + Desafio (50 min)**

| # | Exercício | Tempo | Nível | Notas |
|---|-----------|------:|-------|-------|
| 3 | UDFs e Análise de Plano de Execução | 20 min | 🟡 Intermediário | 6 sub-exercícios: UDF, when/otherwise, Pandas UDF, performance, explain |
| 4 | Análise de Plano de Execução (detalhada) | 10 min | 🟡 Intermediário | 5 exercícios rápidos focados em explain() |
| 5 | Desafio: Otimização com Broadcast + Cache | 20 min | 🔴 Desafio | Pipeline lento para otimizar; requer speedup ≥ 2x |

**Total: 110 min** ✅

---

### Aula 03 — Arquitetura de Data Lake (Medallion)

**Parte 1 — Guiado (60 min)**

| # | Exercício | Tempo | Passos | Complexidade | Notas |
|---|-----------|------:|--------|--------------|-------|
| 1 | Ingestão CSV legado (Parceiro A) | 20 min | 8 passos | Baixa-Média | Encoding ISO-8859-1, separador `;`, to_date, nullValue, metadados |
| 2 | Ingestão JSON + Parquet (Parceiros B e C) | 25 min | 10 passos | Média | JSON multiLine, explode, flatten; Parquet sem config; comparação |
| 3 | Camada Bronze: persistência raw | 15 min | 9 passos | Baixa | Gravar Parquet particionado, round-trip validation |

⚠️ **Exercício 2 (25 min):** Excede 15 min. Cobre 2 parceiros com formatos diferentes (JSON + Parquet), mais comparação de schemas.

**Parte 2 — Intermediário + Desafio (50 min)**

| # | Exercício | Tempo | Nível | Notas |
|---|-----------|------:|-------|-------|
| 4 | Camada Silver: normalização e unificação | 20 min | 🟡 Intermediário | 8 sub-exercícios: renomear colunas, cast, validação, quarentena, union |
| 5 | Persistência particionada por data | 15 min | 🟡 Intermediário | 6 sub-exercícios: partition pruning, dynamic overwrite, coalesce |
| 6 | Desafio: Camada Gold (agregações de negócio) | 15 min | 🔴 Desafio | 3 tabelas Gold: faturamento/estado, top produtos, visão cliente 360° |

**Total: 110 min** ✅

---

### Aula 04 — Introdução ao Apache Airflow

**Parte 1 — Guiado (75 min estimados)**

| # | Exercício | Tempo | Passos | Complexidade | Notas |
|---|-----------|------:|--------|--------------|-------|
| 0 | Setup Airflow + Docker | 10 min | 9 passos | Baixa | Adiciona containers Airflow ao ambiente; espera init ~2 min |
| 1 | Primeira DAG: Pipeline de vendas | 30 min | 12 passos | Média | DAG completa com 3 tasks, XComs, trigger, monitoramento UI |
| 2 | Dependências e XComs avançados | 20 min | 10 passos | Média | Fan-out/fan-in, return como XCom, BashOperator, paralelismo |
| 3 | BashOperator: notificações e comandos | 15 min | 7 passos | Baixa-Média | Templates Jinja, params, exit codes, guard tasks |

⚠️ **Subtotal Parte 1: 75 min — EXCEDE O LIMITE DE 60 min**

⚠️ **Exercício 1 (30 min):** Excede limite de 15 min. Cobre toda a anatomia de uma DAG + trigger + monitoramento na UI. Muito conteúdo para o primeiro contato com Airflow.

⚠️ **Exercício 2 (20 min):** Excede limite de 15 min. Paralelismo, XComs de múltiplas tasks e BashOperator.

**Parte 2 — Intermediário + Desafio (35 min)**

| # | Exercício | Tempo | Nível | Notas |
|---|-----------|------:|-------|-------|
| 4 | Schedule e Template Variables | 15 min | 🟡 Intermediário | 6 sub-exercícios: @daily, {{ ds }}, macros, backfill |
| 5 | Retry e Error Handling | 15 min | 🟡 Intermediário | 5 sub-exercícios: retries, exponential backoff, callbacks, timeout, clear |
| 6 | Desafio: Pipeline completo 8+ tasks | 20 min | 🔴 Desafio | Combina tudo: scheduling, paralelo, XCom, retry, templates |

⚠️ **Desafio (20 min):** Exercício de integração complexo. Alunos podem precisar de mais tempo dependendo do nível.

**Total: 110 min** ✅ (porém distribuição entre partes está desbalanceada: 75/35 em vez de 60/50)

---

### Aula 05 — Airflow Avançado: Branching, Sensors, Spark

**Parte 1 — Guiado (55 min)**

| # | Exercício | Tempo | Passos | Complexidade | Notas |
|---|-----------|------:|--------|--------------|-------|
| 1 | BranchPythonOperator: processamento adaptativo | 20 min | Guiado completo | Média | Branching condicional baseado em volume de dados |
| 2 | FileSensor: esperando dados dos parceiros | 20 min | Guiado completo | Média | Sensor com poke_interval, timeout, mode; espera real ~30s |
| 3 | TaskGroups: organização visual de DAGs | 15 min | Guiado completo | Baixa-Média | Agrupamento de tasks em grupos colapsáveis |

⚠️ **Exercícios 1 e 2 (20 min cada):** Excedem limite de 15 min mas são exercícios guiados completos que introduzem conceitos novos (branching, sensors).

**Parte 2 — Intermediário + Desafio (47 min)**

| # | Exercício | Tempo | Nível | Notas |
|---|-----------|------:|-------|-------|
| 4 | SparkSubmitOperator | 15 min | 🟡 Intermediário | Integração Airflow → Spark, submissão de job |
| 5 | Callbacks e Alertas | 12 min | 🟡 Intermediário | on_failure, on_success, on_retry, alertas |
| 6 | Desafio: Pipeline com Sensor + Branch + Spark | 20 min | 🔴 Desafio | Pipeline E2E combinando tudo da aula |

**Total: 102 min** ✅

---

### Aula 06 — Data Quality: Validação e Monitoramento

**Parte 1 — Guiado (60 min)**

| # | Exercício | Tempo | Passos | Complexidade | Notas |
|---|-----------|------:|--------|--------------|-------|
| 1 | Check de Completude (nulls, campos obrigatórios) | 20 min | Guiado | Média | Funções de verificação, thresholds, relatório |
| 2 | Check de Unicidade (deduplicação) | 20 min | Guiado | Média | Identificar duplicatas, estratégias de dedup |
| 3 | Check de Integridade Referencial | 20 min | Guiado | Média | Anti-join para órfãos, consistência entre tabelas |

⚠️ **Todos os exercícios (20 min cada):** Excedem limite de 15 min. Cada um implementa um tipo de check completo com funções reutilizáveis.

**Parte 2 — Intermediário + Desafio (50 min)**

| # | Exercício | Tempo | Nível | Notas |
|---|-----------|------:|-------|-------|
| 4 | Quarentena de registros inválidos | 15 min | 🟡 Intermediário | Separar válidos/inválidos, metadados de quarentena |
| 5 | Integrar Quality Checks em DAG Airflow | 15 min | 🟡 Intermediário | DAG com branching: qualidade OK → continua, falha → alerta |
| 6 | Desafio: Framework DataQualityFramework | 20 min | 🔴 Desafio | Framework genérico e reutilizável para checks |

**Total: 110 min** ✅

---

### Aula 07 — Produção: Containerização, Logging, Pipeline E2E

**Parte 1 — Guiado (50 min)**

| # | Exercício | Tempo | Passos | Complexidade | Notas |
|---|-----------|------:|--------|--------------|-------|
| 1 | Containerizar Spark Job como script CLI | 20 min | Guiado | Média | argparse, main(), entry point |
| 2 | Logging Estruturado no Spark Job | 15 min | Guiado | Média | Logging JSON, níveis, métricas de execução |
| 3 | Escrita Idempotente (overwrite por partição) | 15 min | Guiado | Média | Dynamic partition overwrite, deduplicação |

⚠️ **Exercício 1 (20 min):** Excede limite de 15 min. Containerização requer mais contexto e setup.

**Parte 2 — Intermediário + Desafio (52 min)**

| # | Exercício | Tempo | Nível | Notas |
|---|-----------|------:|-------|-------|
| 4 | DAG Airflow com SparkSubmitOperator | 15 min | 🟡 Intermediário | Orquestração do job containerizado via Airflow |
| 5 | Quality Checks como Task na DAG | 12 min | 🟡 Intermediário | Adicionar validações pós-processamento |
| 6 | Desafio: Pipeline E2E completo | 25 min | 🔴 Desafio | Docker Compose completo, pipeline end-to-end automatizado |

⚠️ **Desafio (25 min):** Excede o ideal para um único exercício da Parte 2. Alunos podem não completar em tempo hábil.

**Total: 102 min** ✅

---

## 3. Flags de Alerta

### ⚠️ Exercícios que excedem 15 min (Parte 1)

| Aula | Exercício | Tempo | Desvio |
|------|-----------|------:|-------:|
| 01 | Ex. 1 — Spark Básico | 30 min | +15 min |
| 01 | Ex. 2 — Agregações | 20 min | +5 min |
| 02 | Ex. 1 — Joins multi-fonte | 30 min | +15 min |
| 02 | Ex. 2 — Window Functions | 30 min | +15 min |
| 03 | Ex. 2 — Ingestão JSON + Parquet | 25 min | +10 min |
| 04 | Ex. 1 — Primeira DAG | 30 min | +15 min |
| 04 | Ex. 2 — Dependências/XComs | 20 min | +5 min |
| 05 | Ex. 1 — BranchPythonOperator | 20 min | +5 min |
| 05 | Ex. 2 — FileSensor | 20 min | +5 min |
| 06 | Ex. 1 — Check Completude | 20 min | +5 min |
| 06 | Ex. 2 — Check Unicidade | 20 min | +5 min |
| 06 | Ex. 3 — Check Integridade | 20 min | +5 min |
| 07 | Ex. 1 — Containerizar Spark Job | 20 min | +5 min |

### ⚠️ Lab com Parte 1 > 60 min

| Aula | Parte 1 Total | Desvio | Impacto |
|------|-------------:|-------:|---------|
| **Aula 04** | **75 min** | **+15 min** | Parte 2 fica comprimida (35 min em vez de 50 min) |

### ✅ Labs dentro do limite total (≤ 110 min)

Todos os 7 labs estão dentro do limite total de 110 minutos.

---

## 4. Recomendações de Ajuste

### 🔴 Prioridade Alta — Aula 04 (Parte 1 excede 60 min)

**Problema:** Parte 1 soma 75 min (setup 10 + ex.1 30 + ex.2 20 + ex.3 15).

**Opções de ajuste:**

1. **Mover o BashOperator (Ex. 3) para Parte 2:** Reduz Parte 1 para 60 min e Parte 2 ficaria com 50 min (15+15+20 = equilíbrio perfeito).

2. **Reduzir o Exercício 1 para 20 min:** Remover os passos 8-9 (filter vs where, encadeamento) que podem ser absorvidos no exercício seguinte. Parte 1 ficaria em 65 min — ainda ligeiramente acima, mas aceitável.

3. **Criar um README com time budget explícito para Aula 04** indicando que Ex. 1 é o mais longo e sugerindo ao instrutor cobrir passos 1-7 presencialmente e deixar 8-12 como leitura opcional.

**Recomendação:** Opção 1 — mover Ex. 3 (BashOperator) para o início da Parte 2.

---

### 🟡 Prioridade Média — Exercícios de 30 min (Aulas 01, 02, 04)

**Problema:** Vários exercícios guiados têm 30 min, o dobro do limite de 15 min.

**Análise:** Esses exercícios são os "pilares" de cada aula — introduzem conceitos fundamentais que requerem progressão gradual:
- Aula 01, Ex.1: Primeiro contato com Spark (9 passos incrementais)
- Aula 02, Ex.1: 4 tipos de join + explain() + cache
- Aula 02, Ex.2: Window functions (conceito complexo)
- Aula 04, Ex.1: Primeira DAG (12 passos com UI)

**Opções:**

1. **Aceitar como exceção documentada:** São exercícios fundacionais que perdem qualidade se cortados.

2. **Dividir em 2 exercícios de 15 min:** Ex.: "Spark Básico Parte A (SparkSession, leitura, schema)" e "Spark Básico Parte B (filter, select, encadeamento)". Melhora visualmente mas pode fragmentar a narrativa.

3. **Adicionar checkpoints intermediários:** A cada 15 min, incluir um "Checkpoint ✅" no documento para o instrutor saber onde pausar se necessário.

**Recomendação:** Opção 3 — adicionar checkpoints de 15 min dentro dos exercícios longos, mantendo a coesão narrativa mas dando flexibilidade ao instrutor.

---

### 🟢 Prioridade Baixa — Exercícios de 20 min (Aulas 03, 05, 06, 07)

**Problema:** Exercícios guiados de 20 min excedem 15 min por 5 minutos.

**Análise:** Um desvio de 5 min é aceitável considerando:
- Tempo de contexto switching entre exercícios (~2 min)
- Alunos mais lentos compensam com alunos mais rápidos
- O total da Parte 1 permanece ≤ 60 min nessas aulas

**Recomendação:** Nenhuma ação necessária. Monitorar em sala de aula se alunos conseguem completar no tempo. Se não, considerar reduzir 1 passo em exercícios específicos.

---

### 📝 Observações Gerais

1. **Aulas 05 e 07 (102 min total):** Têm 8 min de folga. Podem absorver eventuais atrasos sem impactar o cronograma.

2. **Exercícios de Desafio (🔴):** São explicitamente opcionais em várias aulas. Se o tempo estiver apertado, o instrutor pode atribuí-los como tarefa de casa sem perda de aprendizado presencial.

3. **Tempo de espera (Airflow):** Aulas 04-07 envolvem esperar o Airflow Scheduler detectar DAGs (~30s) e monitorar execuções. Esses "dead times" não são produtivos mas são inevitáveis. Recomendação: instrutor use esses momentos para contextualizar teoria.

4. **Setup (primeira execução):** Em aulas onde Docker precisa baixar imagens (01, 04), o setup pode exceder o previsto em 5-10 min para alunos com conexão lenta. Recomendação: instruir alunos a fazer `docker pull` na noite anterior.

---

## Resumo Executivo

| Métrica | Valor |
|---------|-------|
| Labs dentro do limite total (≤ 110 min) | **7/7** ✅ |
| Labs com Parte 1 ≤ 60 min | **6/7** (Aula 04 excede) |
| Exercícios com > 15 min na Parte 1 | **13 de ~25** |
| Ação corretiva necessária | **1 aula** (Aula 04) |
| Ação recomendada (baixa prioridade) | Checkpoints em exercícios longos |

**Conclusão:** A estrutura temporal dos labs está majoritariamente adequada. O único ajuste necessário é o rebalanceamento da Aula 04 (mover Ex. 3 BashOperator da Parte 1 para Parte 2). Os exercícios longos (30 min) são justificados pela complexidade dos temas mas se beneficiariam de checkpoints intermediários de 15 min.
