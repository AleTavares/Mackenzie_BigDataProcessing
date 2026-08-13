# Guia de Teste dos Labs em GitHub Codespaces

Este guia descreve como testar e validar cada aula do curso de Big Data Processing utilizando GitHub Codespaces.

---

## 1. Como Abrir o Repositório em GitHub Codespaces

### Passo a Passo

1. Acesse o repositório no GitHub: `https://github.com/<seu-usuario>/BigDataProcessing`
2. Clique no botão verde **"<> Code"**
3. Selecione a aba **"Codespaces"**
4. Clique em **"Create codespace on main"** (ou na branch desejada)
5. Selecione o **machine type 4-core** (mínimo recomendado — veja seção de limitações)
6. Aguarde o ambiente ser provisionado:
   - O Docker Compose será iniciado automaticamente
   - O `postCreateCommand` instalará as dependências Python
   - As portas 8080, 8081 e 8888 serão encaminhadas automaticamente
7. Quando o terminal estiver disponível, verifique se os containers estão rodando:
   ```bash
   docker ps
   ```
8. Acesse as interfaces web pela aba **"Ports"** no painel inferior do VS Code

### Configuração Utilizada

O ambiente é definido pelo `.devcontainer/devcontainer.json`:

- **Docker Compose**: `shared/docker-compose.full.yml`
- **Serviço principal**: `jupyter`
- **Workspace**: `/home/jovyan/work`
- **Portas encaminhadas**: 8080 (Spark UI), 8081 (Airflow), 8888 (Jupyter)
- **Requisitos mínimos**: 4 CPUs, 8 GB RAM
- **Usuário remoto**: `jovyan`

---

## 2. Checklist de Validação por Aula

### Aula 1 — Introdução ao Spark

| Item | Verificação | Status |
|------|-------------|--------|
| Ambiente sobe corretamente | `docker ps` mostra containers spark-master, spark-worker, jupyter | ☐ |
| Spark Master UI acessível | Porta 8080 → interface web do Spark | ☐ |
| Jupyter Notebook acessível | Porta 8888 → JupyterLab | ☐ |
| Datasets disponíveis | Verificar `/home/jovyan/work/aula_01/data/` | ☐ |
| Notebooks/scripts executam sem erro | Executar cells do notebook da Aula 1 | ☐ |

### Aula 2 — Spark SQL e Otimização

| Item | Verificação | Status |
|------|-------------|--------|
| Ambiente sobe corretamente | `docker ps` mostra containers ativos | ☐ |
| Spark Master UI acessível | Porta 8080 → interface web do Spark | ☐ |
| Jupyter Notebook acessível | Porta 8888 → JupyterLab | ☐ |
| Datasets disponíveis | Verificar `/home/jovyan/work/aula_02/data/` | ☐ |
| Notebooks/scripts executam sem erro | Executar cells do notebook da Aula 2 | ☐ |

### Aula 3 — Data Lakehouse (Bronze/Silver/Gold)

| Item | Verificação | Status |
|------|-------------|--------|
| Ambiente sobe corretamente | `docker ps` mostra containers ativos | ☐ |
| Spark Master UI acessível | Porta 8080 → interface web do Spark | ☐ |
| Jupyter Notebook acessível | Porta 8888 → JupyterLab | ☐ |
| Datasets disponíveis | Verificar `/home/jovyan/work/aula_03/data/` (CSV, JSON, Parquet) | ☐ |
| Notebooks/scripts executam sem erro | Executar pipeline Bronze → Silver → Gold | ☐ |

### Aula 4 — Apache Airflow Básico

| Item | Verificação | Status |
|------|-------------|--------|
| Ambiente sobe corretamente | `docker ps` mostra containers + airflow | ☐ |
| Spark Master UI acessível | Porta 8080 → interface web do Spark | ☐ |
| Jupyter Notebook acessível | Porta 8888 → JupyterLab | ☐ |
| **Airflow UI acessível** | **Porta 8081 → interface web do Airflow** | ☐ |
| Datasets disponíveis | Verificar `/home/jovyan/work/aula_04/data/` | ☐ |
| DAGs visíveis no Airflow | Verificar se DAGs aparecem na UI | ☐ |
| Notebooks/scripts executam sem erro | Executar DAG de exemplo | ☐ |

### Aula 5 — Airflow + Spark Integration

| Item | Verificação | Status |
|------|-------------|--------|
| Ambiente sobe corretamente | `docker ps` mostra containers + airflow | ☐ |
| Spark Master UI acessível | Porta 8080 → interface web do Spark | ☐ |
| Jupyter Notebook acessível | Porta 8888 → JupyterLab | ☐ |
| **Airflow UI acessível** | **Porta 8081 → interface web do Airflow** | ☐ |
| Datasets disponíveis | Verificar `/home/jovyan/work/aula_05/data/` | ☐ |
| Notebooks/scripts executam sem erro | Executar pipeline Airflow + Spark | ☐ |

### Aula 6 — Streaming e Processamento em Tempo Real

| Item | Verificação | Status |
|------|-------------|--------|
| Ambiente sobe corretamente | `docker ps` mostra containers + airflow | ☐ |
| Spark Master UI acessível | Porta 8080 → interface web do Spark | ☐ |
| Jupyter Notebook acessível | Porta 8888 → JupyterLab | ☐ |
| **Airflow UI acessível** | **Porta 8081 → interface web do Airflow** | ☐ |
| Datasets disponíveis | Verificar `/home/jovyan/work/aula_06/data/` | ☐ |
| Notebooks/scripts executam sem erro | Executar notebook de streaming | ☐ |

### Aula 7 — Projeto Final e Integração

| Item | Verificação | Status |
|------|-------------|--------|
| Ambiente sobe corretamente | `docker ps` mostra containers + airflow | ☐ |
| Spark Master UI acessível | Porta 8080 → interface web do Spark | ☐ |
| Jupyter Notebook acessível | Porta 8888 → JupyterLab | ☐ |
| **Airflow UI acessível** | **Porta 8081 → interface web do Airflow** | ☐ |
| Datasets disponíveis | Verificar `/home/jovyan/work/aula_07/data/` | ☐ |
| Notebooks/scripts executam sem erro | Executar pipeline completo do projeto | ☐ |

---

## 3. Limitações Conhecidas do Codespaces

### Mínimo 4-core machine type recomendado

- O `devcontainer.json` exige **4 CPUs e 8 GB de RAM** (`hostRequirements`)
- Machine types menores (2-core) **não são suficientes** para rodar Spark + Airflow simultaneamente
- Se a opção 4-core não estiver disponível, verifique os limites da sua conta/organização no GitHub

### Timeout de inatividade (30 minutos)

- O Codespace entra em modo **idle** após 30 minutos sem atividade
- Ao retornar, pode ser necessário reiniciar os containers:
  ```bash
  docker compose -f shared/docker-compose.full.yml up -d
  ```
- Para evitar perda de trabalho, salve notebooks frequentemente
- É possível ajustar o timeout em **Settings → Codespaces → Default idle timeout**

### Limitação de recursos vs Docker local

| Aspecto | Codespaces (4-core) | Docker local (típico) |
|---------|---------------------|----------------------|
| CPUs | 4 | 8-16 |
| RAM | 8 GB | 16-64 GB |
| Disco | 32 GB (padrão) | Ilimitado |
| Rede | Latência variável | Local |
| GPU | Não disponível | Depende da máquina |

- Jobs Spark com datasets grandes podem ser **significativamente mais lentos**
- O Airflow pode demorar mais para agendar e executar tasks
- Recomenda-se usar **amostras reduzidas** dos datasets para testes no Codespaces

---

## 4. Troubleshooting Específico de Codespaces

### Port forwarding não funciona

**Sintoma**: Ao clicar na porta na aba "Ports", a página não carrega ou mostra erro de conexão.

**Soluções**:

1. Verifique se o `portsAttributes` está configurado no `devcontainer.json`:
   ```json
   "portsAttributes": {
     "8080": { "label": "Spark Master UI", "onAutoForward": "notify" },
     "8081": { "label": "Airflow UI", "onAutoForward": "notify" },
     "8888": { "label": "Jupyter Notebook", "onAutoForward": "notify" }
   }
   ```
2. Confirme que `forwardPorts` inclui as portas necessárias:
   ```json
   "forwardPorts": [8080, 8081, 8888]
   ```
3. Verifique se o container está escutando na porta correta:
   ```bash
   docker ps --format "table {{.Names}}\t{{.Ports}}"
   ```
4. Tente alterar a visibilidade da porta para **"Public"** na aba Ports (clique com botão direito → Port Visibility)
5. Se nenhuma das opções acima funcionar, reinicie o Codespace

### Container demora para iniciar

**Sintoma**: Após criar o Codespace, os containers não estão prontos e os serviços não respondem.

**Soluções**:

1. **Aguarde o `postCreateCommand` finalizar** — acompanhe no terminal "Creation Log"
2. Verifique o status dos containers:
   ```bash
   docker compose -f shared/docker-compose.full.yml ps
   ```
3. Se containers estiverem em estado `restarting`, verifique os logs:
   ```bash
   docker compose -f shared/docker-compose.full.yml logs --tail=50
   ```
4. Em caso de falha persistente, reconstrua os containers:
   ```bash
   docker compose -f shared/docker-compose.full.yml down
   docker compose -f shared/docker-compose.full.yml up -d
   ```
5. O build inicial das imagens Docker pode levar **5-10 minutos** no primeiro uso — seja paciente

### Volumes não montam corretamente

**Sintoma**: Arquivos esperados (datasets, notebooks, DAGs) não aparecem dentro dos containers.

**Soluções**:

1. **Use caminhos relativos** no `docker-compose.yml` — caminhos absolutos do host local não funcionam no Codespaces:
   ```yaml
   # ✅ Correto (relativo)
   volumes:
     - ./aula_01/data:/home/jovyan/work/aula_01/data

   # ❌ Incorreto (absoluto do host)
   volumes:
     - /home/usuario/projeto/data:/home/jovyan/work/data
   ```
2. Verifique se o workspace está no diretório correto:
   ```bash
   pwd
   # Deve mostrar: /home/jovyan/work
   ```
3. Liste o conteúdo esperado:
   ```bash
   ls -la /home/jovyan/work/aula_*/data/
   ```
4. Se os dados não existirem, pode ser necessário executar um script de setup:
   ```bash
   # Verifique se há script de geração de dados
   find . -name "generate_data*" -o -name "setup_data*"
   ```
5. Confirme que o `workspaceFolder` no `devcontainer.json` aponta para o diretório correto do repositório

### Outros problemas comuns

| Problema | Causa provável | Solução |
|----------|---------------|---------|
| `Permission denied` em arquivos | Diferença de UID entre host e container | Executar como `jovyan` ou ajustar permissões |
| Spark job falha com `OutOfMemoryError` | RAM insuficiente no Codespace | Reduzir tamanho do dataset ou aumentar machine type |
| Airflow não mostra DAGs | Pasta `dags/` não montada corretamente | Verificar volume mount no docker-compose |
| Jupyter mostra "Connection failed" | Kernel ainda inicializando | Aguardar 30s e tentar novamente |
| `docker: command not found` | Feature docker-in-docker não ativou | Rebuild container (`Ctrl+Shift+P` → "Rebuild Container") |

---

## 5. Comandos Úteis para Diagnóstico

```bash
# Verificar containers rodando
docker ps

# Ver logs de um serviço específico
docker compose -f shared/docker-compose.full.yml logs spark-master
docker compose -f shared/docker-compose.full.yml logs airflow
docker compose -f shared/docker-compose.full.yml logs jupyter

# Reiniciar todos os serviços
docker compose -f shared/docker-compose.full.yml restart

# Verificar uso de recursos
docker stats --no-stream

# Verificar portas em uso
ss -tlnp | grep -E '8080|8081|8888'

# Testar conectividade com serviço
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080
curl -s -o /dev/null -w "%{http_code}" http://localhost:8888
```

---

## 6. Checklist Rápido Pós-Criação do Codespace

Use este checklist logo após o Codespace ser criado:

- [ ] Terminal disponível e responsivo
- [ ] `docker ps` mostra containers rodando
- [ ] Porta 8080 (Spark UI) acessível na aba Ports
- [ ] Porta 8888 (Jupyter) acessível na aba Ports
- [ ] Porta 8081 (Airflow) acessível na aba Ports (se aplicável)
- [ ] Arquivos do repositório visíveis em `/home/jovyan/work/`
- [ ] `python --version` retorna versão esperada
- [ ] `spark-submit --version` retorna versão do Spark

---

*Última atualização: Junho 2025*
