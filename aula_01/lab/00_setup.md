# Lab Setup - Aula 1: Fundamentos de Big Data e Apache Spark

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Antes de trabalharmos com os dados de vendas da DataFlow, precisamos garantir que o ambiente de desenvolvimento está funcionando. Vamos configurar um cluster Spark local com Docker — o mesmo tipo de setup que usamos em produção, só que em escala reduzida."

## Pré-requisitos

Antes de iniciar, verifique que você possui os seguintes itens instalados e configurados:

| Requisito | Versão Mínima | Como Verificar |
|-----------|---------------|----------------|
| Docker Desktop (Windows/Mac) ou Docker Engine (Linux) | 24.0+ | `docker --version` |
| Docker Compose v2 | 2.20+ | `docker compose version` |
| Git | 2.30+ | `git --version` |
| RAM disponível | 8 GB | Docker Desktop → Settings → Resources |
| CPU cores | 4 cores | Docker Desktop → Settings → Resources |

> **⚠️ Importante:** No Docker Desktop (Windows/Mac), vá em **Settings → Resources** e configure pelo menos **8 GB de RAM** e **4 cores de CPU** para o Docker. Sem isso, o Spark Worker pode falhar ao inicializar.

---

## Passo 1: Clonar o Repositório

**Descrição:** Baixar o código-fonte do curso para sua máquina local.

**Comando:**
```bash
git clone git@github.com:AleTavares/Mackenzie_Mackenzie_BigDataProcessing.git
```

**Resultado esperado:**
```
Cloning into 'Mackenzie_Mackenzie_BigDataProcessing'...
remote: Enumerating objects: ...
Resolving deltas: 100% (...), done.
```

**Explicação:** O repositório contém todos os materiais do curso: notebooks, datasets, configurações Docker e scripts auxiliares. Se você já clonou anteriormente, pule este passo e execute `git pull` para atualizar.

**Dica:** Se já possui o repositório clonado, basta fazer `git pull origin main` para pegar as últimas atualizações.

---

## Passo 2: Navegar para a Raiz do Projeto

**Descrição:** Entrar no diretório do projeto. Todos os comandos do curso devem ser executados a partir da raiz.

**Comando:**
```bash
cd Mackenzie_Mackenzie_BigDataProcessing
```

**Resultado esperado:**
```
~/Mackenzie_BigDataProcessing $
```

**Explicação:** Os scripts e arquivos Docker Compose do curso usam caminhos relativos. Executar comandos fora da raiz causará erros de "arquivo não encontrado".

**Dica:** Você pode confirmar que está no diretório correto com `ls shared/docker-compose.yml` — se o arquivo existir, está no lugar certo.

---

## Passo 3: Subir o Ambiente Base com Docker Compose

**Descrição:** Iniciar os containers do cluster Spark (Master + Worker) e do Jupyter Notebook.

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
[+] Running 4/4
 ✔ Network dataflow-network    Created
 ✔ Container spark-master      Started
 ✔ Container spark-worker      Started
 ✔ Container jupyter-notebook  Started
```

**Explicação:** Este comando cria três containers:
- **spark-master**: Gerenciador do cluster Spark (coordena os jobs)
- **spark-worker**: Nó de processamento (executa as tarefas distribuídas)
- **jupyter-notebook**: Interface interativa onde você escreverá código PySpark

A flag `-d` executa os containers em background (detached mode), liberando o terminal.

**Dica:** Na primeira execução, o Docker precisa baixar as imagens (~2 GB total). Isso pode levar 5-10 minutos dependendo da sua conexão. Nas próximas vezes, será instantâneo.

---

## Passo 4: Verificar que os Containers Estão Rodando

**Descrição:** Confirmar que todos os serviços inicializaram corretamente.

**Comando:**
```bash
docker compose -f shared/docker-compose.yml ps
```

**Resultado esperado:**
```
NAME               IMAGE                          STATUS          PORTS
spark-master       bitnami/spark:3.5             Up (healthy)    0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
spark-worker       bitnami/spark:3.5             Up              
jupyter-notebook   jupyter/pyspark-notebook      Up              0.0.0.0:8888->8888/tcp
```

**Explicação:** Todos os três containers devem exibir status **"Up"**. O spark-master possui um healthcheck configurado — quando exibir **"Up (healthy)"**, significa que a interface web já está acessível. O spark-worker depende do master estar healthy antes de iniciar.

**Dica:** Se algum container mostrar "Restarting" ou "Exit", veja os logs com `docker compose -f shared/docker-compose.yml logs <nome-do-container>` para diagnosticar o problema.

---

## Passo 5: Acessar o Jupyter Notebook

**Descrição:** Abrir a interface do Jupyter Notebook no navegador e verificar que os datasets estão acessíveis.

**Comando:**
```
Abra o navegador e acesse: http://localhost:8888
```

**Resultado esperado:**
- A interface do Jupyter Notebook abre **sem solicitar token ou senha**
- No painel lateral de arquivos, deve haver uma pasta **`data/`** visível
- Dentro de `data/`, devem existir os datasets do curso (ex: `vendas_2023.csv`, `produtos.csv`)

**Explicação:** O Jupyter está configurado sem autenticação (token vazio) para facilitar o acesso no laboratório local. A pasta `data/` é montada em modo somente-leitura a partir do diretório `datasets/` do repositório — isso garante que os dados originais não sejam alterados acidentalmente durante os exercícios.

**Dica:** Se a página não carregar, aguarde ~30 segundos após o Passo 3. O Jupyter pode levar um tempo para inicializar completamente.

---

## Passo 6: Verificar o Spark Master UI

**Descrição:** Acessar a interface de administração do Spark para confirmar que o cluster está operacional.

**Comando:**
```
Abra o navegador e acesse: http://localhost:8080
```

**Resultado esperado:**
- A página exibe **"Spark Master at spark://spark-master:7077"**
- Na seção **"Workers"**, deve haver **1 worker registrado**
- O worker exibe: 2 cores, 2.0 GB de memória disponível
- Status do worker: **ALIVE**

**Explicação:** O Spark Master UI é a central de monitoramento do cluster. Aqui você pode acompanhar workers conectados, jobs em execução, uso de memória e CPU. Em produção, teríamos dezenas ou centenas de workers — no nosso lab, um é suficiente para aprender os conceitos.

**Dica:** Se nenhum worker aparecer, aguarde ~60 segundos. O worker só se registra após o master estar completamente healthy (healthcheck passa).

---

## Passo 7: Testar Conexão com o Spark

**Descrição:** Criar um novo notebook no Jupyter e verificar que o PySpark consegue se conectar ao cluster Spark.

**Comando (no Jupyter):**

1. No Jupyter, clique em **"New" → "Python 3 (ipykernel)"**
2. Na primeira célula do notebook, cole e execute o seguinte código:

```python
from pyspark.sql import SparkSession

# Criar SparkSession conectada ao cluster
spark = SparkSession.builder \
    .appName("DataFlow-Lab-Setup-Teste") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

# Verificar versão e conexão
print(f"✅ Spark conectado com sucesso!")
print(f"   Versão: {spark.version}")
print(f"   App Name: {spark.sparkContext.appName}")
print(f"   Master: {spark.sparkContext.master}")

# Teste rápido: criar um DataFrame simples
df = spark.createDataFrame(
    [(1, "teste"), (2, "setup"), (3, "ok")],
    ["id", "status"]
)
df.show()

print(f"   DataFrame criado com {df.count()} linhas ✅")
```

**Resultado esperado:**
```
✅ Spark conectado com sucesso!
   Versão: 3.5.x
   App Name: DataFlow-Lab-Setup-Teste
   Master: spark://spark-master:7077
+---+------+
| id|status|
+---+------+
|  1| teste|
|  2| setup|
|  3|    ok|
+---+------+

   DataFrame criado com 3 linhas ✅
```

**Explicação:** A SparkSession é o ponto de entrada principal para toda interação com o Spark. O `.master("spark://spark-master:7077")` conecta nosso notebook ao cluster Docker. Se esse teste passar, significa que:
- O Jupyter consegue se comunicar com o Spark Master
- O Spark Master consegue delegar tarefas ao Worker
- O ambiente está 100% funcional para os exercícios do lab

**Dica:** Se a célula travar por mais de 2 minutos, verifique se o spark-worker está registrado no Master UI (Passo 6). Sem worker disponível, os jobs ficam na fila indefinidamente.

---

## Passo 8: Encerrar o Ambiente (Pós-Lab)

**Descrição:** Ao final do laboratório, parar todos os containers para liberar recursos da máquina.

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
[+] Running 4/4
 ✔ Container jupyter-notebook  Removed
 ✔ Container spark-worker      Removed
 ✔ Container spark-master      Removed
 ✔ Network dataflow-network    Removed
```

**Explicação:** O comando `down` para e remove os containers, liberando memória e CPU. Os notebooks que você salvou ficam persistidos no diretório `shared/notebooks/` (volume montado), então não serão perdidos. Na próxima aula, basta executar `docker compose up -d` novamente.

**Dica:** Se quiser apenas pausar (sem remover containers), use `docker compose -f shared/docker-compose.yml stop`. Para reiniciar depois: `docker compose -f shared/docker-compose.yml start`.

---

## Troubleshooting

### Problema: "Port already in use" (Porta já em uso)

**Sintoma:** Erro ao subir os containers mencionando que a porta 8080, 8888 ou 7077 já está em uso.

**Solução:**
```bash
# Identificar o processo usando a porta (exemplo: porta 8080)
# Linux/Mac:
lsof -i :8080
# Windows (PowerShell):
netstat -ano | findstr :8080

# Encerrar o processo ou alterar a porta no docker-compose.yml
# Para alterar a porta do Jupyter para 9999, edite shared/docker-compose.yml:
# ports: "9999:8888"
```

---

### Problema: "Docker daemon is not running"

**Sintoma:** Comando `docker compose up` retorna erro de conexão com o daemon.

**Solução:**
```bash
# Windows/Mac: Abra o Docker Desktop e aguarde o ícone ficar verde
# Linux: Inicie o serviço Docker
sudo systemctl start docker

# Verifique se está rodando:
docker info
```

---

### Problema: "Not enough memory" / Container OOM (Out of Memory)

**Sintoma:** O spark-worker reinicia repetidamente ou exibe "Killed" nos logs.

**Solução:**
1. **Docker Desktop (Windows/Mac):**
   - Vá em **Settings → Resources → Advanced**
   - Aumente a RAM para **8 GB** (mínimo) ou **12 GB** (recomendado)
   - Clique em **Apply & Restart**

2. **Linux:** Verifique a memória disponível com `free -h`. Se tiver menos de 8 GB livre, feche outros programas.

3. **Alternativa:** Reduza a memória do worker editando `shared/docker-compose.yml`:
   ```yaml
   # De:
   - SPARK_WORKER_MEMORY=2g
   # Para:
   - SPARK_WORKER_MEMORY=1g
   ```

---

### Problema: Container falha ao iniciar (status "Restarting")

**Sintoma:** `docker compose ps` mostra container em loop de restart.

**Solução:**
```bash
# Ver logs do container com problema:
docker compose -f shared/docker-compose.yml logs spark-worker

# Causas comuns:
# 1. Spark Master ainda não está pronto → aguarde ~60 segundos e tente novamente
# 2. Conflito de rede → remova redes antigas:
docker network prune

# 3. Imagem corrompida → force o download novamente:
docker compose -f shared/docker-compose.yml pull
docker compose -f shared/docker-compose.yml up -d --force-recreate
```

---

### Problema: Jupyter não encontra a pasta `data/`

**Sintoma:** Ao abrir o Jupyter, a pasta `data/` não aparece ou está vazia.

**Solução:**
```bash
# Verifique se os datasets foram gerados:
ls datasets/

# Se a pasta estiver vazia, gere os datasets:
python datasets/gerar_datasets.py

# Reinicie o container do Jupyter:
docker compose -f shared/docker-compose.yml restart jupyter
```

---

### Problema: SparkSession não conecta ao cluster

**Sintoma:** A célula do notebook trava indefinidamente ao criar SparkSession.

**Solução:**
```bash
# 1. Verifique se o worker está registrado:
#    Acesse http://localhost:8080 → seção "Workers" deve mostrar 1 worker ALIVE

# 2. Verifique a rede entre containers:
docker exec jupyter-notebook ping -c 3 spark-master

# 3. Se falhar, recrie a rede:
docker compose -f shared/docker-compose.yml down
docker compose -f shared/docker-compose.yml up -d

# 4. No notebook, use master "local[*]" como alternativa temporária:
# spark = SparkSession.builder.master("local[*]").getOrCreate()
```

---

## Checklist de Validação

Antes de prosseguir para os exercícios do lab, confirme que todos os itens abaixo estão ✅:

- [ ] Docker está rodando (`docker info` sem erros)
- [ ] Três containers estão "Up" (`docker compose ps`)
- [ ] Spark Master UI acessível em http://localhost:8080
- [ ] 1 worker registrado e ALIVE no Spark Master UI
- [ ] Jupyter Notebook acessível em http://localhost:8888
- [ ] Pasta `data/` visível no Jupyter com datasets
- [ ] SparkSession conecta ao cluster e cria DataFrame com sucesso

> **Carlos:** "Ambiente pronto! Agora podemos focar no que importa: processar os dados de vendas da DataFlow. Vamos para o primeiro exercício."
