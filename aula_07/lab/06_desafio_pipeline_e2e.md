# Exercício 6 — Docker Compose Completo com Pipeline E2E Automatizado

## Duração Estimada

⏱️ ~25 minutos

## Contexto

> **Roberto Tanaka (CEO):** "Marina, semana que vem tenho reunião com os investidores. Preciso mostrar o pipeline rodando sozinho — do zero. Quero ligar o sistema, dados entrarem, e no final ver o relatório na camada Gold atualizado. Sem ninguém tocando em nada. Se não conseguirmos demonstrar isso de forma convincente, perdemos a rodada de investimento."

> **Marina Silva (CTO):** "Entendido, Roberto. Carlos, precisamos de um Docker Compose de produção que suba tudo: cluster Spark, Airflow completo, volumes compartilhados. O investidor vai ver um `docker compose up` e em poucos minutos o pipeline inteiro roda — sensor detecta arquivo, Spark processa, quality checks validam, dados chegam na Gold. Tudo automatizado."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Vou montar o `docker-compose.producao.yml` que integra tudo que construímos: o `pipeline_vendas.py` containerizado (Exercício 01), com logging estruturado (Exercício 02), escrita idempotente (Exercício 03), orquestrado pela DAG com FileSensor e SparkSubmit (Exercício 04), e validado pelos quality checks (Exercício 05). Um único arquivo que demonstra o pipeline end-to-end."

> **Ana Rodrigues (Product Owner):** "E o relatório de qualidade? Quero que os investidores vejam que não é só velocidade — é velocidade com governança. Dados validados antes de chegar na Gold."

> **Carlos Mendes:** "Sim, Ana. Os quality checks rodam automaticamente como parte do pipeline. Se algum dado sujo passar, o pipeline para e alerta. É exatamente o que vamos demonstrar."

## Objetivos

Ao final deste exercício, você será capaz de:

- Criar um Docker Compose de produção com múltiplos serviços integrados
- Configurar um cluster Spark (master + worker) containerizado
- Configurar Airflow (webserver + scheduler) com DAGs montadas via volume
- Definir volumes compartilhados para comunicação entre serviços
- Configurar variáveis de ambiente para conexão entre serviços
- Demonstrar um pipeline end-to-end rodando automaticamente
- Validar que dados fluem corretamente: incoming → Bronze → Silver → Gold

## Pré-requisitos

- Exercícios 01 a 05 desta aula concluídos (pipeline completo com DAG e quality checks)
- Aula 04 e 05 — conceitos de Airflow (DAGs, Operators, Sensors)
- Conhecimento básico de Docker Compose (services, volumes, networks)
- Docker e Docker Compose instalados no ambiente

---

## O que você vai construir

Um Docker Compose de produção que integra todos os componentes do pipeline:

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│  docker-compose.producao.yml                                                              │
│                                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │  Network: dataflow_network                                                          │  │
│  │                                                                                     │  │
│  │  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────────────────────┐  │  │
│  │  │  spark-master    │   │  spark-worker    │   │  airflow-webserver              │  │  │
│  │  │  porta: 8080     │   │  2 cores, 2G    │   │  porta: 8081                    │  │  │
│  │  │  spark://master  │   │  conecta master  │   │  UI de monitoramento            │  │  │
│  │  │  :7077           │   │                  │   │                                 │  │  │
│  │  └─────────────────┘   └─────────────────┘   └─────────────────────────────────┘  │  │
│  │                                                                                     │  │
│  │  ┌───────────────────────────────┐   ┌───────────────────────────────────────────┐ │  │
│  │  │  airflow-scheduler             │   │  airflow-init                             │ │  │
│  │  │  Executa DAGs no schedule      │   │  Inicializa DB + cria admin user          │ │  │
│  │  │  Monitora FileSensor           │   │  Configura conexão spark_default          │ │  │
│  │  │  Submete SparkSubmitOperator   │   │                                           │ │  │
│  │  └───────────────────────────────┘   └───────────────────────────────────────────┘ │  │
│  │                                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                           │
│  Volumes Compartilhados:                                                                  │
│    ./code/spark_jobs → /opt/spark/jobs     (scripts Spark acessíveis pelo cluster)        │
│    ./code/dags       → /opt/airflow/dags   (DAGs acessíveis pelo scheduler)               │
│    ./data            → /opt/spark/data     (dados compartilhados entre serviços)           │
│    airflow_logs      → /opt/airflow/logs   (logs persistentes do Airflow)                 │
│                                                                                           │
│  Fluxo Automatizado:                                                                      │
│    1. Airflow scheduler inicia → carrega DAG                                              │
│    2. FileSensor detecta arquivo em incoming/{{ ds }}/                                     │
│    3. SparkSubmitOperator submete pipeline_vendas.py ao spark-master                      │
│    4. Spark processa: incoming → Bronze → Silver → Gold                                   │
│    5. Quality checks validam partição Gold                                                │
│    6. Notificação confirma sucesso                                                        │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Por que Docker Compose para demonstrar o pipeline?**

| Aspecto | Sem Docker Compose | Com Docker Compose |
|---------|-------------------|-------------------|
| Setup | Instalar Spark, Airflow, configs manuais | `docker compose up` |
| Reprodutibilidade | "Funciona na minha máquina" | Funciona em qualquer máquina |
| Demo para investidores | 30min de setup | 2min de startup |
| Networking | Configurar IPs, portas, firewalls | Rede interna automática |
| Volumes | Copiar arquivos manualmente | Mapeamento declarativo |
| Cleanup | Desinstalar tudo | `docker compose down -v` |

---

## Problema

Crie o arquivo `aula_07/code/docker-compose.producao.yml` que configura um ambiente de produção completo com os seguintes requisitos:

### Serviços obrigatórios:

1. **spark-master**: Spark Master standalone com Web UI exposta
2. **spark-worker**: Spark Worker conectado ao master com recursos definidos (cores, memória)
3. **airflow-webserver**: Airflow Web UI para monitoramento
4. **airflow-scheduler**: Airflow Scheduler que executa as DAGs
5. **airflow-init**: Serviço de inicialização (cria DB, admin user, conexão spark_default)

### Volumes compartilhados:

- Scripts Spark (`pipeline_vendas.py`) acessíveis pelo worker
- DAGs (`dag_pipeline_vendas.py`) acessíveis pelo scheduler
- Dados compartilhados (incoming, bronze, silver, gold) entre Spark e Airflow
- Logs persistentes do Airflow

### Configurações de rede:

- Todos os serviços na mesma rede Docker
- Airflow consegue submeter jobs ao `spark://spark-master:7077`
- Portas expostas: Spark UI (8080), Airflow UI (8081)

### Script de demonstração:

Crie também um script `aula_07/code/demo_pipeline.sh` que:
1. Sobe o Docker Compose
2. Aguarda serviços ficarem healthy
3. Copia dados de exemplo para o diretório `incoming/`
4. Monitora o Airflow até o pipeline completar (ou timeout)
5. Exibe os dados resultantes na camada Gold

---

## Dicas

### Dica 1: Estrutura do Docker Compose

O arquivo YAML precisa de: version, services, volumes e networks. Comece com a estrutura base:

```yaml
version: "3.8"

services:
  spark-master:
    # ...
  spark-worker:
    # ...
  airflow-webserver:
    # ...
  airflow-scheduler:
    # ...
  airflow-init:
    # ...

volumes:
  airflow_logs:
  airflow_db:

networks:
  dataflow_network:
    driver: bridge
```

### Dica 2: Spark Master e Worker

Use a imagem oficial `bitnami/spark` ou `apache/spark`. O master expõe a porta 7077 (Spark protocol) e 8080 (Web UI). O worker precisa das variáveis `SPARK_MASTER_URL`, `SPARK_WORKER_CORES` e `SPARK_WORKER_MEMORY`:

```yaml
spark-master:
  image: bitnami/spark:3.5
  environment:
    - SPARK_MODE=master
  ports:
    - "8080:8080"   # Web UI
    - "7077:7077"   # Spark protocol
```

O worker precisa dos dados e scripts montados para executar:

```yaml
spark-worker:
  image: bitnami/spark:3.5
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_MEMORY=2g
  volumes:
    - ./spark_jobs:/opt/spark/jobs
    - ../data:/opt/spark/data
```

### Dica 3: Airflow com SQLite e LocalExecutor

Para uma demo simplificada, use `apache/airflow` com `LocalExecutor` e SQLite (não precisa de Postgres para o lab). As variáveis essenciais:

```yaml
airflow-scheduler:
  image: apache/airflow:2.8.1
  environment:
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
    - AIRFLOW__CORE__LOAD_EXAMPLES=False
  volumes:
    - ./dags:/opt/airflow/dags
    - airflow_logs:/opt/airflow/logs
```

O webserver usa a mesma imagem mas com comando diferente:

```yaml
command: airflow webserver --port 8081
```

### Dica 4: Inicialização do Airflow (airflow-init)

O serviço de inicialização precisa criar o banco, o usuário admin e configurar a conexão com Spark:

```yaml
airflow-init:
  image: apache/airflow:2.8.1
  entrypoint: /bin/bash
  command: >
    -c "
    airflow db init &&
    airflow users create --username admin --password admin
      --firstname Admin --lastname User --role Admin --email admin@dataflow.com &&
    airflow connections add spark_default
      --conn-type spark --conn-host spark://spark-master --conn-port 7077
    "
```

Use `depends_on` para garantir que o init roda antes do scheduler e webserver.

### Dica 5: Volumes e comunicação entre serviços

O ponto crucial é que **Spark e Airflow precisam acessar os mesmos dados**. O mapeamento de volumes garante isso:

```
Host                        Container (Spark)         Container (Airflow)
─────────────────────────   ─────────────────────    ─────────────────────
./code/spark_jobs/       → /opt/spark/jobs/         (spark-submit lê daqui)
./code/dags/             →                           /opt/airflow/dags/
./data/aula_07/producao/ → /opt/spark/data/         /opt/spark/data/
```

O SparkSubmitOperator na DAG referencia o path **dentro do container**: `/opt/spark/jobs/pipeline_vendas.py`

### Dica 6: Script de demonstração

O script de demo deve ser prático para apresentar ao investidor:

```bash
#!/bin/bash
echo "🚀 DataFlow Analytics — Demo Pipeline E2E"
echo "==========================================="

# 1. Subir serviços
docker compose -f docker-compose.producao.yml up -d

# 2. Aguardar serviços (health check básico)
echo "⏳ Aguardando serviços..."
sleep 30  # Ou loop com curl até Airflow responder

# 3. Simular chegada de dados
DATA_REF=$(date +%Y-%m-%d)
mkdir -p data/aula_07/producao/incoming/${DATA_REF}/
cp data/aula_07/producao/sample/vendas.parquet \
   data/aula_07/producao/incoming/${DATA_REF}/vendas.parquet

echo "📂 Dados copiados para incoming/${DATA_REF}/"

# 4. Trigger manual da DAG (para não esperar schedule)
docker exec airflow-scheduler airflow dags trigger \
    dataflow_pipeline_vendas_producao --conf '{"data_ref":"'${DATA_REF}'"}'

# 5. Monitorar execução
echo "⏳ Monitorando execução..."
# ... aguardar conclusão ...

# 6. Verificar resultado
echo "📊 Verificando camada Gold..."
ls -la data/aula_07/datalake/gold/metricas_vendas/
```

### Dica 7: depends_on e ordem de inicialização

A ordem de startup importa. O Airflow precisa do banco inicializado antes de rodar, e o Spark worker precisa do master:

```yaml
spark-worker:
  depends_on:
    - spark-master

airflow-scheduler:
  depends_on:
    airflow-init:
      condition: service_completed_successfully

airflow-webserver:
  depends_on:
    airflow-init:
      condition: service_completed_successfully
```

---

## Critérios de Validação

Verifique se sua implementação atende a **todos** os critérios:

| # | Critério | Como verificar |
|---|----------|----------------|
| 1 | Arquivo `docker-compose.producao.yml` criado em `aula_07/code/` | `ls aula_07/code/docker-compose.producao.yml` |
| 2 | Serviço `spark-master` com porta 8080 exposta e modo master | Inspecionar YAML |
| 3 | Serviço `spark-worker` conectado ao master com cores e memória definidos | Variáveis `SPARK_MASTER_URL`, `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY` |
| 4 | Serviço `airflow-webserver` com porta exposta (8081) | Inspecionar ports |
| 5 | Serviço `airflow-scheduler` com `LOAD_EXAMPLES=False` | Não poluir com DAGs de exemplo |
| 6 | Serviço `airflow-init` que cria DB, admin user e conexão spark_default | Entrypoint com `db init`, `users create`, `connections add` |
| 7 | Volume montando `spark_jobs/` para container Spark | Worker acessa `pipeline_vendas.py` |
| 8 | Volume montando `dags/` para container Airflow | Scheduler carrega `dag_pipeline_vendas.py` |
| 9 | Volume compartilhado de dados entre Spark e Airflow | Ambos acessam `/opt/spark/data` |
| 10 | Rede Docker definida e usada por todos os serviços | `networks: dataflow_network` |
| 11 | `depends_on` configurado corretamente (init antes de scheduler/webserver) | Ordem de inicialização correta |
| 12 | Script `demo_pipeline.sh` criado com fluxo completo | Sobe, espera, copia dados, trigger, verifica |
| 13 | Script de demo é executável e tem shebang | `#!/bin/bash` e `chmod +x` |
| 14 | `docker compose config` valida o YAML sem erros | Sintaxe correta |
| 15 | Pipeline end-to-end: dados fluem incoming → Bronze → Silver → Gold | Demonstrar com `demo_pipeline.sh` |

---

## Teste sua Implementação

**1. Validar sintaxe do Docker Compose:**
```bash
docker compose -f aula_07/code/docker-compose.producao.yml config --quiet
```
Se não houver output, o YAML é válido.

**2. Subir o ambiente:**
```bash
docker compose -f aula_07/code/docker-compose.producao.yml up -d
```

**3. Verificar serviços rodando:**
```bash
docker compose -f aula_07/code/docker-compose.producao.yml ps
```
Todos os serviços devem estar `Up` ou `healthy`.

**4. Acessar UIs:**
- Spark Master: http://localhost:8080
- Airflow: http://localhost:8081 (admin/admin)

**5. Executar demo completo:**
```bash
chmod +x aula_07/code/demo_pipeline.sh
./aula_07/code/demo_pipeline.sh
```

**6. Verificar resultado final:**
```bash
ls -la data/aula_07/datalake/gold/metricas_vendas/
```
Deve conter partição `data_ref=YYYY-MM-DD/` com arquivos `.parquet`.

**7. Cleanup:**
```bash
docker compose -f aula_07/code/docker-compose.producao.yml down -v
```

---

## Conceitos Consolidados

Este exercício integra **todos os conceitos** da Aula 07 e aulas anteriores:

| Conceito | Aula de Origem | Aplicação Aqui |
|----------|----------------|----------------|
| Docker Compose multi-serviço | Aula 01 - Setup inicial | Compose de produção com 5 serviços |
| Spark standalone cluster | Aula 01 - Ambiente Docker | Master + Worker containerizados |
| Script CLI com argparse | Aula 07 - Exercício 01 | `pipeline_vendas.py` montado via volume |
| Logging estruturado | Aula 07 - Exercício 02 | Logs do Spark acessíveis nos containers |
| Escrita idempotente | Aula 07 - Exercício 03 | Retries seguros no pipeline automatizado |
| DAG com FileSensor + SparkSubmit | Aula 07 - Exercício 04 | DAG orquestrando via Airflow containerizado |
| Quality checks no pipeline | Aula 07 - Exercício 05 | Validação automática antes da Gold |
| Arquitetura Medallion | Aula 03 - Pipeline completo | incoming → Bronze → Silver → Gold |
| Conexões e Operators | Aula 05 - SparkSubmitOperator | `spark_default` configurada no init |
| Volumes Docker | Infra base do curso | Compartilhamento de dados entre serviços |

---

## Reflexão

Antes de concluir, considere:

1. **Escalabilidade:** se o volume de dados triplicar amanhã, o que muda no Docker Compose? (Dica: adicionar mais workers, ajustar memória)
2. **Segurança:** em produção real, o que faria diferente com credenciais? (Dica: Docker secrets, variáveis de ambiente em `.env`, não hardcodar senhas)
3. **Monitoramento:** como saber se o pipeline está saudável sem abrir a UI? (Dica: healthchecks no Compose, métricas Prometheus, alertas)
4. **CI/CD:** como integrar esse Docker Compose em um pipeline de deploy? (Dica: GitHub Actions que roda `docker compose up` em staging antes de produção)
5. **Reprocessamento:** se o investidor perguntar "e se precisar reprocessar o mês inteiro?", qual seria a resposta? (Dica: backfill do Airflow + idempotência garante segurança)

---

## Parabéns! 🎉

Você completou o desafio final da Aula 07! Ao criar este Docker Compose de produção, você demonstrou domínio completo sobre:

- **Containerização** de jobs Spark para produção
- **Orquestração** com Airflow de pipelines complexos
- **Qualidade de dados** integrada ao fluxo automatizado
- **Infraestrutura como código** com Docker Compose
- **Pipeline end-to-end** que funciona sem intervenção humana

> **Roberto Tanaka (CEO):** "Carlos, Marina — é exatamente isso que eu queria. Um `docker compose up` e em minutos o investidor vê dados fluindo, sendo processados, validados e entregues. Isso demonstra maturidade técnica. Obrigado, equipe."

> **Marina Silva (CTO):** "Esse é o poder de engenharia de dados bem feita. Não é sobre ter as ferramentas mais caras — é sobre integrar as peças certas de forma robusta, reproduzível e automatizada."

---

## Próximo Passo

Na **Aula 08**, sua equipe vai aplicar TODOS esses conceitos em um **projeto final** real — escolhendo uma vertical de negócio, construindo um pipeline completo, e apresentando para a turma (e para o "Roberto"). O Docker Compose de produção que você construiu aqui será a base da arquitetura do projeto final.
