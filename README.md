# Big Data Processing

**MBA em Engenharia de Dados — Universidade Mackenzie**

Disciplina prática de processamento de dados em larga escala utilizando Apache Spark, Apache Airflow e Docker. O curso acompanha a evolução da empresa fictícia **DataFlow Analytics**, contextualizando os desafios técnicos em cenários reais de engenharia de dados.

## Visão Geral do Curso

| Item | Detalhe |
|------|---------|
| Carga horária | 32 horas (8 aulas × 4 horas) |
| Formato | Teoria (slides PDF) + Laboratório hands-on |
| Linguagem | Português do Brasil |
| Público-alvo | Profissionais MBA com experiência em programação |

### Estrutura das Aulas

Cada aula de 4 horas segue a distribuição:

| Bloco |
|-------|
| Contexto narrativo |
| Teoria (Slides HTML) |
| Demonstração ao vivo |
| Intervalo |
| Introdução ao Lab |
| Lab Parte 1 (guiado) |
| Lab Parte 2 (intermediário + desafio) |
| Encerramento e discussão |

### Blocos Temáticos

| Bloco | Aulas | Tema |
|-------|-------|------|
| Processamento | 1–3 | Fundamentos e transformações com Apache Spark |
| Orquestração | 4–5 | Automação de pipelines com Apache Airflow |
| Produção | 6–7 | Qualidade de dados e pipelines end-to-end |
| Integração | 8 | Projeto final em grupo |

## Narrativa do Curso — A DataFlow Analytics

A **DataFlow Analytics** é uma startup brasileira de inteligência de dados para e-commerce, fundada em 2021 em São Paulo por Marina Silva (CTO) e Roberto Tanaka (CEO). Com um time enxuto de 6 pessoas, a empresa conquistou seu primeiro grande cliente: a **ShopBrasil**, uma rede de e-commerce com 200 lojas online.

No início, tudo funcionava bem com Python, pandas, PostgreSQL e scripts cron. Mas o volume de dados começou a crescer exponencialmente — 100 mil registros viraram 1 milhão, novos parceiros de marketplace entraram, a Black Friday se aproximava. O que antes rodava em minutos passou a levar horas. E então, numa segunda-feira de manhã, o inevitável aconteceu: o script morreu.

Ao longo deste curso, você faz parte da equipe de engenharia da DataFlow. Semana a semana, os desafios escalam: migrar de pandas para processamento distribuído, integrar fontes heterogêneas, automatizar pipelines, garantir qualidade de dados e, finalmente, entregar um pipeline de produção completo — containerizado, orquestrado e monitorado.

Cada aula apresenta um problema real que a equipe enfrenta, e o lab é a sua solução.

---

### Aula 1 — "O script morreu"

Carlos Mendes (Engenheiro de Dados Sênior) chega ao escritório numa segunda-feira e encontra o temido `Killed` no terminal. O relatório de vendas da ShopBrasil — 100 mil registros — matou o script pandas após 4h37min. Marina (CTO) decide: é hora de migrar para processamento distribuído. **Nesta aula**, os alunos aprendem os fundamentos do Apache Spark e fazem sua primeira análise de faturamento usando PySpark.

### Aula 2 — "Black Friday se aproxima"

A DataFlow cresceu 10x. Ana (Product Owner) precisa de relatórios que cruzam vendas com clientes e categorias de produtos para a estratégia de Black Friday. Carlos precisa dominar joins complexos e análises avançadas. **Nesta aula**, os alunos aprendem joins distribuídos, window functions (ranking, tendências) e UDFs para classificação de clientes.

### Aula 3 — "Três parceiros, três formatos"

A ShopBrasil integrou novos parceiros de marketplace. Cada um envia dados em formato diferente: CSV legado com encoding ISO-8859-1, JSON de API paginada, e Parquet otimizado. Marina define a arquitetura Medallion para organizar o data lake. **Nesta aula**, os alunos constroem um pipeline de ingestão multi-formato com camadas Bronze → Silver → Gold, incluindo quarentena de dados inválidos.

### Aula 4 — "Cron não escala"

Os scripts cron começam a falhar silenciosamente. Quando uma etapa quebra, ninguém percebe até o relatório atrasar. Roberto (CEO) exige automação confiável. Carlos propõe Apache Airflow. **Nesta aula**, os alunos aprendem a criar DAGs com PythonOperator, BashOperator, XComs e template variables — a primeira automação real do pipeline.

### Aula 5 — "O pipeline precisa ser inteligente"

Nem todo dia é igual: segundas têm volume alto (acúmulo do fim de semana), sextas precisam de relatório semanal. O pipeline precisa tomar decisões automaticamente. **Nesta aula**, os alunos implementam branching condicional, FileSensors para detectar dados chegando, TaskGroups para organização e integração Airflow + Spark via SparkSubmitOperator.

### Aula 6 — "Dados ruins, decisões ruins"

Um bug silencioso: 15% dos registros de um parceiro chegam com valores negativos e campos nulos. O relatório foi para o board com números errados. Marina decreta: nenhum dado entra em produção sem validação. **Nesta aula**, os alunos implementam um framework de qualidade de dados com checks de completude, unicidade e validade, sistema de quarentena e relatório consolidado.

### Aula 7 — "Preparando para produção"

O pipeline funciona no notebook, mas "funcionar no notebook" não é produção. Marina exige: scripts parametrizados, logging estruturado, escrita idempotente, orquestração automática e tudo containerizado. **Nesta aula**, os alunos integram tudo: script Spark com CLI + DAG Airflow + Docker Compose + quality checks — o pipeline end-to-end completo.

### Aula 8 — "Reunião do conselho"

A DataFlow vai apresentar seus projetos ao board de investidores. Cada equipe de engenharia demonstra seu pipeline ao vivo. Não basta slides bonitos — os investidores querem ver o produto funcionando. **Nesta aula**, os grupos apresentam seus projetos finais com demo ao vivo: `docker compose up` → pipeline rodando end-to-end.

## Tecnologias

| Tecnologia | Versão | Uso |
|------------|--------|-----|
| Apache Spark (PySpark) | 3.5.x | Processamento distribuído de dados |
| Apache Airflow | 2.8.x | Orquestração de pipelines |
| Docker Compose | 2.x | Infraestrutura local de laboratório |
| Python | 3.11+ | Linguagem principal (PySpark, DAGs, scripts) |
| Jupyter Notebook | latest | Ambiente interativo para labs |

## Pré-requisitos

- **Python**: nível intermediário (funções, classes, bibliotecas como pandas)
- **SQL**: nível básico (SELECT, JOIN, GROUP BY, WHERE)
- **Docker**: conceitos básicos (containers, imagens, volumes, docker-compose up/down)
- **Hardware mínimo**: 8 GB RAM, 4 cores de CPU, 20 GB de espaço em disco

## Setup do Ambiente

O ambiente pode ser configurado de duas formas:

### Opção 1: Docker local (recomendado)

**Requisitos**: Docker Desktop instalado com Docker Compose v2.

```bash
# Clonar o repositório
git clone git@github.com:AleTavares/Mackenzie_BigDataProcessing.git
cd Mackenzie_BigDataProcessing

# Subir ambiente básico (Spark + Jupyter) — Aulas 1-3
docker compose -f shared/docker-compose.yml up -d

# Subir ambiente com Airflow — Aulas 4-5
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d

# Subir stack completa (Spark + Airflow + Jupyter) — Aulas 6-7
docker compose -f shared/docker-compose.full.yml up -d
```

### Opção 2: GitHub Codespaces

1. Abra o repositório no GitHub
2. Clique em **Code** → **Codespaces** → **Create codespace on main**
3. O ambiente será configurado automaticamente via `.devcontainer/devcontainer.json`
4. Aguarde o build do container (primeira vez pode levar alguns minutos)

### Portas de Serviço

| Serviço | Porta | URL |
|---------|-------|-----|
| Spark UI | 8080 | http://localhost:8080 |
| Airflow Webserver | 8081 | http://localhost:8081 |
| Jupyter Notebook | 8888 | http://localhost:8888 |

### Comandos Rápidos

```bash
# Iniciar ambiente
./shared/start_env.sh

# Parar ambiente
./shared/stop_env.sh

# Resetar dados para estado inicial (útil entre labs)
./shared/reset_data.sh

# Verificar status dos containers
docker compose ps
```

## Estrutura do Repositório

```
Mackenzie_BigDataProcessing/
├── README.md                  # Este arquivo
├── aula_01/                   # Fundamentos de Big Data e Apache Spark
│   ├── slides/                # HTML com teoria
│   ├── lab/                   # Laboratório hands-on (markdown + código)
│   └── src/                   # Código-fonte dos exercícios
├── aula_02/                   # Transformações Avançadas com Spark
├── aula_03/                   # Ingestão e Persistência de Dados
├── aula_04/                   # Introdução ao Apache Airflow
├── aula_05/                   # Orquestração Avançada com Airflow
├── aula_06/                   # Qualidade de Dados e Monitoramento
├── aula_07/                   # Pipeline End-to-End em Produção
├── aula_08/                   # Projeto Final (apresentações em grupo)
│   └── PROJETO_FINAL.md       # Regras para entrega do Trabalho Final
├── shared/                    # Configurações compartilhadas
│   ├── docker-compose.yml     # Ambiente base (Spark + Jupyter)
│   ├── docker-compose.airflow.yml  # Override com Airflow
│   ├── docker-compose.full.yml     # Stack completa
│   ├── start_env.sh           # Script para iniciar ambiente
│   ├── stop_env.sh            # Script para parar ambiente
│   └── reset_data.sh          # Script para resetar dados
├── datasets/                  # Dados sintéticos por aula
│   ├── aula_01/               # ~100K registros de vendas (CSV)
│   ├── aula_02/               # ~1M registros (Parquet, JSON)
│   ├── aula_03/               # Dados de 3 parceiros (multi-formato)
│   ├── aula_04/               # Vendas diárias particionadas
│   ├── aula_05/               # Múltiplas origens para orquestração
│   ├── aula_06/               # Dados com problemas de qualidade
│   └── aula_07/               # Dataset completo para pipeline E2E
├── .devcontainer/             # Configuração GitHub Codespaces
│   └── devcontainer.json
└── requirements.txt           # Dependências Python fixadas
```

## Ementa por Aula

### Aula 1 — Fundamentos de Big Data e Apache Spark
- Big Data e os 5 V's
- Arquitetura do Apache Spark (Driver, Executors, Cluster Manager)
- SparkSession, leitura de dados, DataFrame API
- Operações: groupBy, agregações (sum, avg, count), orderBy

### Aula 2 — Transformações Avançadas com Spark
- Joins: inner, left, right, full, cross, semi, anti
- Window Functions: row_number, rank, dense_rank, lag, lead
- UDFs (User Defined Functions)
- Plano de execução e otimização (Catalyst Optimizer)

### Aula 3 — Ingestão e Persistência de Dados
- Leitura multi-formato: CSV, JSON, Parquet
- Schema management (evolution vs enforcement)
- Particionamento de dados
- Arquitetura Medallion: Bronze → Silver → Gold

### Aula 4 — Introdução ao Apache Airflow
- Conceitos: DAG, Task, Operator, Sensor
- PythonOperator, BashOperator
- Dependências entre tasks e XComs
- Schedule intervals e template variables

### Aula 5 — Orquestração Avançada com Airflow
- BranchPythonOperator (branching condicional)
- Sensors: FileSensor, ExternalTaskSensor
- TaskGroups para organização
- SparkSubmitOperator: integração Airflow + Spark
- Callbacks de falha e trigger_rule

### Aula 6 — Qualidade de Dados e Monitoramento
- Dimensões de qualidade (completude, unicidade, integridade)
- Validações customizadas com PySpark
- Sistema de quarentena para dados inválidos
- Monitoramento e alertas no Airflow

### Aula 7 — Pipeline End-to-End em Produção
- Containerização de Spark jobs
- Logging estruturado e observabilidade
- Escrita idempotente
- Pipeline completo: Spark + Airflow + Docker + Quality checks

### Aula 8 — Projeto Final
- Apresentação em grupo (3-5 integrantes)
- Requisitos: pipeline PySpark, DAG Airflow, Docker Compose, qualidade de dados, arquitetura medallion
- Demonstração ao vivo obrigatória
- Avaliação: funcionamento (30%), arquitetura (25%), apresentação (20%), qualidade (15%), documentação (10%)

## Licença

Material didático de uso exclusivo para a disciplina Big Data Processing do MBA em Engenharia de Dados — Universidade Mackenzie.
