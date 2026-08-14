# Lab Setup - Aula 1: Fundamentos de Big Data e Apache Spark

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Antes de trabalharmos com os dados de vendas da DataFlow, precisamos garantir que o ambiente de desenvolvimento está funcionando. Vamos configurar um ambiente Spark local com Docker — prático e leve para aprender os fundamentos."

## Pré-requisitos

Antes de iniciar, verifique que você possui os seguintes itens instalados e configurados:

| Requisito | Versão Mínima | Como Verificar |
|-----------|---------------|----------------|
| Docker Desktop (Windows/Mac) ou Docker Engine (Linux) | 24.0+ | `docker --version` |
| Docker Compose v2 | 2.20+ | `docker compose version` |
| Git | 2.30+ | `git --version` |
| RAM disponível | 8 GB | Docker Desktop → Settings → Resources |
| CPU cores | 4 cores | Docker Desktop → Settings → Resources |

> **⚠️ Importante:** No Docker Desktop (Windows/Mac), vá em **Settings → Resources** e configure pelo menos **8 GB de RAM** e **4 cores de CPU** para o Docker.

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

**Descrição:** Iniciar o container do Jupyter Notebook com PySpark embutido (Spark em modo local).

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

**Explicação:** Este comando cria um container:
- **jupyter-spark**: Jupyter Notebook com PySpark embutido, rodando Spark em modo local (`local[*]`)

O Spark em modo local utiliza todos os cores disponíveis da máquina para processar dados. Não há containers separados de Spark Master ou Worker — tudo roda dentro do mesmo container. Isso é suficiente para os labs do curso com datasets de até 1M de registros.

A flag `-d` executa o container em background (detached mode), liberando o terminal.

**Dica:** Na primeira execução, o Docker precisa baixar a imagem (~2 GB). Isso pode levar 5-10 minutos dependendo da sua conexão. Nas próximas vezes, será instantâneo.

---

## Passo 4: Verificar que o Container Está Rodando

**Descrição:** Confirmar que o serviço inicializou corretamente.

**Comando:**
```bash
docker compose -f shared/docker-compose.yml ps
```

**Resultado esperado:**
```
NAME               IMAGE                                    STATUS     PORTS
jupyter-spark      quay.io/jupyter/pyspark-notebook:latest  Up         0.0.0.0:8888->8888/tcp, 0.0.0.0:4040->4040/tcp
```

**Explicação:** O container deve exibir status **"Up"**. A porta 8888 é o Jupyter Notebook e a porta 4040 é a Spark App UI (que só fica ativa quando um job Spark está executando).

**Dica:** Se o container mostrar "Restarting" ou "Exit", veja os logs com `docker compose -f shared/docker-compose.yml logs jupyter` para diagnosticar o problema.

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

## Passo 6: Verificar a Spark App UI (Opcional)

**Descrição:** A Spark App UI fica disponível na porta 4040 **apenas enquanto um SparkSession está ativo** (ou seja, enquanto um notebook com Spark está rodando). Não se preocupe se ela não estiver acessível agora — ela aparecerá no Passo 7 quando criarmos o SparkSession.

**Comando:**
```
Abra o navegador e acesse: http://localhost:4040
(Só funciona após executar código Spark no notebook)
```

**Resultado esperado (após Passo 7):**
- A página exibe informações sobre o SparkSession ativo
- Mostra jobs executados, stages, e detalhes de storage

**Explicação:** Diferente de um cluster Spark standalone (que possui uma Master UI permanente na porta 8080), no modo local a interface de monitoramento é a **Spark App UI** — ela existe por aplicação e só fica ativa durante a execução. Isso é normal e esperado para este ambiente de laboratório.

---

## Passo 7: Testar Conexão com o Spark

**Descrição:** Criar um novo notebook no Jupyter e verificar que o PySpark está funcionando corretamente.

**Comando (no Jupyter):**

1. No Jupyter, clique em **"New" → "Python 3 (ipykernel)"**
2. Na primeira célula do notebook, cole e execute o seguinte código:

```python
from pyspark.sql import SparkSession

# Criar SparkSession em modo local (usa todos os cores disponíveis)
spark = SparkSession.builder \
    .appName("DataFlow-Lab-Setup-Teste") \
    .master("local[*]") \
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
   Master: local[*]
+---+------+
| id|status|
+---+------+
|  1| teste|
|  2| setup|
|  3|    ok|
+---+------+

   DataFrame criado com 3 linhas ✅
```

**Explicação:** A SparkSession é o ponto de entrada principal para toda interação com o Spark. O `.master("local[*]")` indica que o Spark usará todos os cores do container para processamento paralelo. Se esse teste passar, o ambiente está 100% funcional para os exercícios do lab.

**Dica:** Após executar esse código, a Spark App UI estará disponível em http://localhost:4040. Abra em outra aba para explorar!

---

## Passo 8: Encerrar o Ambiente (Pós-Lab)

**Descrição:** Ao final do laboratório, parar o container para liberar recursos da máquina.

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

**Explicação:** O comando `down` para e remove o container, liberando memória e CPU. Os notebooks que você salvou ficam persistidos no diretório `shared/notebooks/` (volume montado), então não serão perdidos. Na próxima aula, basta executar `docker compose up -d` novamente.

**Dica:** Se quiser apenas pausar (sem remover o container), use `docker compose -f shared/docker-compose.yml stop`. Para reiniciar depois: `docker compose -f shared/docker-compose.yml start`.

---

## Troubleshooting

### Problema: "Port already in use" (Porta já em uso)

**Sintoma:** Erro ao subir o container mencionando que a porta 8888 ou 4040 já está em uso.

**Solução:**
```bash
# Identificar o processo usando a porta (exemplo: porta 8888)
# Linux/Mac:
lsof -i :8888
# Windows (PowerShell):
netstat -ano | findstr :8888

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

**Sintoma:** O container reinicia repetidamente ou exibe "Killed" nos logs.

**Solução:**
1. **Docker Desktop (Windows/Mac):**
   - Vá em **Settings → Resources → Advanced**
   - Aumente a RAM para **8 GB** (mínimo) ou **12 GB** (recomendado)
   - Clique em **Apply & Restart**

2. **Linux:** Verifique a memória disponível com `free -h`. Se tiver menos de 8 GB livre, feche outros programas.

---

### Problema: Container falha ao iniciar (status "Restarting")

**Sintoma:** `docker compose ps` mostra container em loop de restart.

**Solução:**
```bash
# Ver logs do container com problema:
docker compose -f shared/docker-compose.yml logs jupyter

# Causas comuns:
# 1. Conflito de rede → remova redes antigas:
docker network prune

# 2. Imagem corrompida → force o download novamente:
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

### Problema: Spark App UI (porta 4040) não acessível

**Sintoma:** Ao abrir http://localhost:4040, a página não carrega.

**Solução:**
Isso é **normal** se você não tem nenhum notebook com SparkSession ativo. A porta 4040 só fica disponível enquanto um SparkSession está rodando. Execute o código do Passo 7 e tente novamente.

```bash
# Verificar se a porta está publicada:
docker port jupyter-spark

# Deve mostrar:
# 4040/tcp -> 0.0.0.0:4040
# 8888/tcp -> 0.0.0.0:8888
```

---

## Checklist de Validação

Antes de prosseguir para os exercícios do lab, confirme que todos os itens abaixo estão ✅:

- [ ] Docker está rodando (`docker info` sem erros)
- [ ] Container `jupyter-spark` está "Up" (`docker compose ps`)
- [ ] Jupyter Notebook acessível em http://localhost:8888
- [ ] Pasta `data/` visível no Jupyter com datasets
- [ ] SparkSession local cria DataFrame com sucesso (Passo 7)
- [ ] Spark App UI acessível em http://localhost:4040 (após executar Passo 7)

> **Carlos:** "Ambiente pronto! Agora podemos focar no que importa: processar os dados de vendas da DataFlow. Vamos para o primeiro exercício."
