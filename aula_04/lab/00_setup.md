# Lab Setup - Aula 4: Introdução ao Apache Airflow

## Duração Estimada

⏱️ ~10 minutos

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Marina escolheu o Apache Airflow para automatizar nossos pipelines. Antes de criar nossa primeira DAG, precisamos adicionar o Airflow ao ambiente Docker que já temos rodando com Spark e Jupyter. A boa notícia: vamos usar um arquivo override — o ambiente base continua intacto."

> **Marina Silva (CTO):** "O Airflow precisa de um banco de dados para armazenar metadados das execuções — histórico de DAGs, status de tasks, logs. Vamos usar SQLite para manter o lab simples. Em produção usaríamos PostgreSQL, mas para aprender os conceitos, SQLite é suficiente."

## O que estamos adicionando?

O ambiente base (Aulas 1-3) já possui:

| Serviço | Porta | Função |
|---------|-------|--------|
| Spark Master | 8080 | Gerenciador do cluster Spark |
| Spark Worker | — | Nó de processamento |
| Jupyter Notebook | 8888 | Interface interativa |

Agora vamos **adicionar** ao ambiente:

| Serviço | Porta | Função |
|---------|-------|--------|
| Airflow Init | — | Inicializa banco e cria usuário admin (roda uma vez) |
| Airflow Webserver | **8081** | Interface web para gerenciar DAGs |
| Airflow Scheduler | — | Agendador que executa as DAGs |

> **⚠️ Nota:** O Airflow Webserver usa a porta **8081** (não 8080) para evitar conflito com o Spark Master UI. Guarde isso!

---

## Pré-requisitos

Antes de iniciar, confirme que:

| Requisito | Como verificar |
|-----------|---------------|
| Ambiente base rodando (Spark + Jupyter) | `docker compose -f shared/docker-compose.yml ps` → 3 containers "Up" |
| Docker com 8 GB RAM alocados | Docker Desktop → Settings → Resources |
| Raiz do projeto como diretório de trabalho | `ls shared/docker-compose.airflow.yml` → arquivo existe |

> **Dica:** Se o ambiente base não estiver rodando, suba-o primeiro com `docker compose -f shared/docker-compose.yml up -d` e aguarde o Spark Master ficar healthy.

---

## Passo 1: Verificar o Ambiente Base

**Descrição:** Confirmar que o Spark e Jupyter estão funcionando antes de adicionar o Airflow.

**Comando:**
```bash
docker compose -f shared/docker-compose.yml ps
```

**Resultado esperado:**
```
NAME               IMAGE                      STATUS          PORTS
spark-master       bitnami/spark:3.5         Up (healthy)    0.0.0.0:8080->8080/tcp, 0.0.0.0:7077->7077/tcp
spark-worker       bitnami/spark:3.5         Up              
jupyter-notebook   jupyter/pyspark-notebook  Up              0.0.0.0:8888->8888/tcp
```

**Explicação:** Precisamos que o ambiente base esteja rodando porque o Airflow vai compartilhar a mesma rede Docker (`dataflow-network`). Isso permite que, futuramente, o Airflow envie jobs diretamente para o cluster Spark.

---

## Passo 2: Conhecer o Arquivo Override do Airflow

**Descrição:** Antes de executar, vamos entender o que o arquivo `docker-compose.airflow.yml` faz.

**Comando:**
```bash
cat shared/docker-compose.airflow.yml
```

**O que esse arquivo configura:**

- **`airflow-init`** — Serviço de inicialização que roda apenas uma vez:
  - Executa `airflow db init` para criar as tabelas de metadados
  - Cria o usuário administrador (login: `admin` / senha: `admin`)
  - Encerra automaticamente após finalizar

- **`airflow-webserver`** — Interface web (porta 8081):
  - Permite visualizar, monitorar e acionar DAGs
  - Depende do `airflow-init` terminar com sucesso

- **`airflow-scheduler`** — Motor de execução:
  - Monitora a pasta `dags/` e executa tasks conforme agendamento
  - Depende do webserver estar healthy

- **Configurações importantes:**
  - `AIRFLOW__CORE__LOAD_EXAMPLES=False` → Não carrega DAGs de exemplo (ambiente limpo)
  - `AIRFLOW__CORE__EXECUTOR=LocalExecutor` → Execução local (adequado para lab)
  - Banco SQLite compartilhado via volume `airflow-data`

> **Carlos:** "O conceito de 'override' no Docker Compose é poderoso — podemos adicionar novos serviços sem modificar o arquivo base. Assim, quem quiser usar só o Spark continua com o arquivo original."

---

## Passo 3: Criar a Pasta de DAGs

**Descrição:** Criar o diretório onde colocaremos nossos arquivos de DAG durante os labs. O Airflow Scheduler monitora essa pasta continuamente.

**Comando:**
```bash
mkdir -p shared/dags
mkdir -p aula_04/code/dags
```

**Resultado esperado:**
```
(nenhuma saída — diretórios criados silenciosamente)
```

**Explicação:** Duas pastas são criadas:
- `shared/dags/` — Pasta geral montada no container como `/opt/airflow/dags/`
- `aula_04/code/dags/` — Pasta específica da Aula 4, montada como subdiretório `/opt/airflow/dags/aula_04/`

Quando você salvar um arquivo `.py` com uma DAG em qualquer uma dessas pastas, o Scheduler vai detectá-lo automaticamente (pode levar até 30 segundos).

**Verificação:**
```bash
ls -la shared/dags/
ls -la aula_04/code/dags/
```

---

## Passo 4: Subir o Airflow com Docker Compose Override

**Descrição:** Iniciar os serviços do Airflow usando o arquivo override em conjunto com o compose base.

**Comando:**
```bash
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d
```

**Resultado esperado:**
```
[+] Running 6/6
 ✔ Volume "bigdataprocessing_airflow-data"  Created
 ✔ Container spark-master                   Running
 ✔ Container spark-worker                   Running
 ✔ Container jupyter-notebook               Running
 ✔ Container airflow-init                   Started
 ✔ Container airflow-webserver              Waiting
 ✔ Container airflow-scheduler              Waiting
```

**Explicação:** O Docker Compose mescla os dois arquivos YAML: mantém os serviços do `docker-compose.yml` (Spark + Jupyter) e adiciona os novos serviços do `docker-compose.airflow.yml` (Airflow). Os containers de Spark que já estavam rodando continuam intactos — apenas os novos são criados.

> **⚠️ Importante:** Na primeira execução, o Docker precisa baixar a imagem do Airflow (~1.5 GB). Isso pode levar 3-5 minutos dependendo da conexão.

---

## Passo 5: Aguardar a Inicialização do Airflow

**Descrição:** O Airflow precisa inicializar o banco de dados e criar o usuário admin antes do webserver ficar disponível. Vamos acompanhar esse processo.

**Comando (acompanhar o init):**
```bash
docker logs -f airflow-init
```

**Resultado esperado (aguarde até ver a mensagem final):**
```
DB: sqlite:////opt/airflow/airflow.db
Performing upgrade to the metadata database...
...
Running upgrade ... -> ...
...
[2024-xx-xx] {manager.py} INFO - Added user admin
User "admin" created with role "Admin"
✅ Airflow inicializado com sucesso! Usuário admin/admin criado.
```

**Para sair do log:** Pressione `Ctrl+C` após ver a mensagem de sucesso.

**Explicação:** O `airflow-init` executa duas operações:
1. `airflow db init` — Cria todas as tabelas de metadados no SQLite
2. `airflow users create` — Registra o usuário `admin` com senha `admin`

Após finalizar, o webserver e o scheduler iniciam automaticamente (dependência configurada no Docker Compose).

**Dica:** Se quiser verificar sem acompanhar o log, use: `docker inspect --format='{{.State.Status}}' airflow-init` — quando retornar `exited` com código 0, a inicialização foi bem-sucedida.

---

## Passo 6: Verificar que Todos os Serviços Estão Rodando

**Descrição:** Confirmar que o ambiente completo (Spark + Airflow) está operacional.

**Comando:**
```bash
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml ps
```

**Resultado esperado:**
```
NAME                IMAGE                          STATUS                     PORTS
airflow-init        apache/airflow:2.8-python3.11  Exited (0)                 
airflow-scheduler   apache/airflow:2.8-python3.11  Up                         
airflow-webserver   apache/airflow:2.8-python3.11  Up (healthy)               0.0.0.0:8081->8080/tcp
jupyter-notebook    jupyter/pyspark-notebook       Up                         0.0.0.0:8888->8888/tcp
spark-master        bitnami/spark:3.5              Up (healthy)               0.0.0.0:8080->8080/tcp, 0.0.0.0:7077->7077/tcp
spark-worker        bitnami/spark:3.5              Up                         
```

**Explicação:** 
- `airflow-init` com status **"Exited (0)"** é **normal** — ele cumpriu sua função e encerrou
- `airflow-webserver` deve exibir **"Up (healthy)"** — significa que o healthcheck passou
- `airflow-scheduler` deve exibir **"Up"** — está monitorando a pasta de DAGs

> **Carlos:** "O airflow-init com 'Exited (0)' não é um erro! Código 0 significa que finalizou com sucesso. É um container de inicialização — roda uma vez e sai."

---

## Passo 7: Acessar o Airflow UI

**Descrição:** Abrir a interface web do Airflow e fazer login.

**Comando:**
```
Abra o navegador e acesse: http://localhost:8081
```

**Credenciais de login:**
| Campo | Valor |
|-------|-------|
| Username | `admin` |
| Password | `admin` |

**Resultado esperado:**
- Página de login do Airflow aparece
- Após login, a página de DAGs é exibida
- A lista de DAGs está **vazia** (nenhuma DAG cadastrada ainda)
- No topo da página, o status do Scheduler aparece como "Running"

**Explicação:** A interface web do Airflow é onde você vai:
- Visualizar DAGs e suas dependências em formato de grafo
- Monitorar execuções (sucesso, falha, retry)
- Acionar DAGs manualmente (trigger)
- Consultar logs de cada task individualmente

> **Marina:** "Perceba que a lista está vazia — é proposital. Configuramos `LOAD_EXAMPLES=False` para não poluir o ambiente com DAGs de demonstração. Na próxima etapa do lab, vamos criar nossa primeira DAG do zero."

---

## Passo 8: Verificar que a Pasta de DAGs Está Montada Corretamente

**Descrição:** Confirmar que o Scheduler está conseguindo acessar a pasta onde colocaremos nossos arquivos de DAG.

**Comando:**
```bash
docker exec airflow-scheduler ls -la /opt/airflow/dags/
```

**Resultado esperado:**
```
total 0
drwxr-xr-x  3 airflow root ... .
drwxr-xr-x  1 airflow root ... ..
drwxr-xr-x  2 airflow root ... aula_04
```

**Explicação:** A pasta `/opt/airflow/dags/` dentro do container está mapeada para:
- `shared/dags/` → montada como `/opt/airflow/dags/`
- `aula_04/code/dags/` → montada como `/opt/airflow/dags/aula_04/`

Quando criarmos uma DAG em `aula_04/code/dags/minha_dag.py` na nossa máquina, ela aparecerá automaticamente em `/opt/airflow/dags/aula_04/minha_dag.py` dentro do container.

**Teste de montagem (criar arquivo de teste):**
```bash
echo "# teste de montagem" > aula_04/code/dags/teste_montagem.py
docker exec airflow-scheduler ls /opt/airflow/dags/aula_04/
```

**Resultado esperado:**
```
teste_montagem.py
```

**Limpeza (remover arquivo de teste):**
```bash
rm aula_04/code/dags/teste_montagem.py
```

> **Carlos:** "Excelente! Isso confirma que qualquer arquivo Python que colocarmos na pasta `aula_04/code/dags/` aparece automaticamente dentro do container. É assim que vamos desenvolver nossas DAGs — editando localmente e o Scheduler detecta as mudanças."

---

## Passo 9: Verificar o Scheduler no Airflow UI

**Descrição:** Confirmar pela interface web que o Scheduler está ativo e saudável.

**Passos no navegador:**

1. Acesse http://localhost:8081 (já logado como admin)
2. Na barra superior, observe o indicador de **"Scheduler"** — deve exibir um ícone verde ✅
3. Opcionalmente, vá em **Admin → Configuration** para ver as configurações ativas

**Verificação via terminal (alternativa):**
```bash
docker exec airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname ""
```

**Resultado esperado:**
```
Found 1 alive jobs.
```

**Explicação:** O Scheduler é o coração do Airflow. Ele:
- Verifica a pasta de DAGs a cada ~30 segundos procurando arquivos novos ou alterados
- Agenda execuções conforme o `schedule_interval` definido em cada DAG
- Despacha tasks para execução quando suas dependências são atendidas

Se o Scheduler parar, nenhuma DAG será executada — mesmo que esteja marcada como ativa no UI.

---

## Troubleshooting

### Problema: Porta 8081 já em uso

**Sintoma:** Erro ao subir o Airflow: `Bind for 0.0.0.0:8081 failed: port is already allocated`

**Solução:**
```bash
# Identificar o processo usando a porta 8081
# Linux/Mac:
lsof -i :8081
# Windows (PowerShell):
netstat -ano | findstr :8081

# Alternativa: alterar a porta no docker-compose.airflow.yml
# Mude "8081:8080" para "8082:8080" e acesse em http://localhost:8082
```

---

### Problema: Airflow Webserver não inicia (fica "Waiting" ou "Restarting")

**Sintoma:** O container `airflow-webserver` reinicia em loop ou não sai do status "Waiting".

**Solução:**
```bash
# 1. Verificar se o airflow-init finalizou com sucesso:
docker inspect --format='{{.State.ExitCode}}' airflow-init
# Deve retornar: 0

# 2. Se retornou código diferente de 0, ver o log do init:
docker logs airflow-init

# 3. Causa comum: volume corrompido. Solução — recriar tudo:
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml down -v
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d

# O flag -v remove os volumes (airflow-data), forçando nova inicialização
```

---

### Problema: Airflow Init falha com erro de banco de dados

**Sintoma:** `docker logs airflow-init` mostra erro de SQLite ou "database is locked".

**Solução:**
```bash
# Remover o volume de dados do Airflow e reiniciar:
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml down
docker volume rm bigdataprocessing_airflow-data 2>/dev/null
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d
```

---

### Problema: DAGs não aparecem no Airflow UI

**Sintoma:** Você criou um arquivo `.py` na pasta de DAGs mas ele não aparece na interface web.

**Solução:**
```bash
# 1. Verificar se o arquivo está montado dentro do container:
docker exec airflow-scheduler ls /opt/airflow/dags/aula_04/

# 2. Verificar se o arquivo tem erros de importação:
docker exec airflow-scheduler python /opt/airflow/dags/aula_04/seu_arquivo.py

# 3. Aguardar até 30 segundos — o Scheduler faz scan periódico

# 4. Forçar re-scan (se urgente):
docker exec airflow-scheduler airflow dags reserialize

# 5. Causa comum: arquivo sem objeto DAG no escopo global
# Verifique que seu arquivo contém algo como:
# with DAG("minha_dag", ...) as dag:
#     ...
# ou
# dag = DAG("minha_dag", ...)
```

---

### Problema: Scheduler mostra status "Not Running" no UI

**Sintoma:** Na interface web, o indicador do Scheduler aparece em vermelho ou com warning.

**Solução:**
```bash
# 1. Verificar se o container está rodando:
docker ps | grep airflow-scheduler

# 2. Ver logs do scheduler para identificar erros:
docker logs airflow-scheduler --tail 50

# 3. Reiniciar apenas o scheduler:
docker restart airflow-scheduler

# 4. Se persistir, reiniciar todo o ambiente Airflow:
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml restart airflow-webserver airflow-scheduler
```

---

### Problema: Rede "dataflow-network" não encontrada

**Sintoma:** Erro `network dataflow-network declared as external, but could not be found`

**Solução:**
```bash
# O ambiente base (Spark) precisa estar rodando primeiro!
# Ele cria a rede. Solução:
docker compose -f shared/docker-compose.yml up -d
# Aguarde o Spark subir, depois:
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d
```

---

## Checklist de Validação

Antes de prosseguir para os exercícios do lab, confirme que todos os itens abaixo estão ✅:

- [ ] Ambiente base (Spark + Jupyter) rodando (`docker compose ps` → 3 containers "Up")
- [ ] Pasta `shared/dags/` criada
- [ ] Pasta `aula_04/code/dags/` criada
- [ ] `airflow-init` finalizou com sucesso (status "Exited (0)")
- [ ] `airflow-webserver` está "Up (healthy)"
- [ ] `airflow-scheduler` está "Up"
- [ ] Airflow UI acessível em http://localhost:8081
- [ ] Login com admin/admin funciona
- [ ] Lista de DAGs está vazia (sem DAGs de exemplo)
- [ ] Pasta de DAGs montada corretamente no container

> **Carlos:** "Ambiente completo! Temos Spark para processamento e Airflow para orquestração — exatamente a mesma combinação que empresas como a DataFlow usam em produção. Agora vamos criar nossa primeira DAG."

---

## Resumo dos Serviços e Portas

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| Spark Master UI | http://localhost:8080 | — |
| Jupyter Notebook | http://localhost:8888 | — (sem token) |
| **Airflow UI** | **http://localhost:8081** | **admin / admin** |

**Comando para subir tudo (Spark + Airflow):**
```bash
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d
```

**Comando para derrubar tudo:**
```bash
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml down
```
