# Lab Setup (Alternativo) — Aula 4 no AWS Fargate (AWS Academy)

## Quando usar este guia

⏱️ ~20 minutos (na primeira vez)

Use este caminho **em vez do `00_setup.md`** quando você **não conseguir rodar o Docker localmente** (bloqueio de rede/máquina na faculdade, pouca RAM, etc.). Aqui subimos o ambiente da Aula 4 — **Jupyter + PySpark e o Airflow** — no **AWS ECS Fargate**, dentro do **AWS Academy Learner Lab**, e acessamos tudo pelo navegador.

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Nem todo mundo consegue rodar Docker na máquina da faculdade. A boa notícia é que o mesmo ambiente sobe na nuvem com um comando. Vamos usar Terraform para provisionar e derrubar tudo em minutos — e o melhor: o Airflow já vem com a DAG da aula carregada."

> **Marina Silva (CTO):** "Provisionar infraestrutura como código é o padrão do mercado. Vocês vão ver na prática como um `terraform apply` cria um cluster inteiro, e um `terraform destroy` devolve tudo. Isso é engenharia de dados de verdade."

---

## O que este ambiente sobe

| Serviço | Porta | Função |
|---------|-------|--------|
| Jupyter Notebook + PySpark | 8888 | Interface interativa (Spark em modo `local[*]`) |
| **Airflow UI** | **8081** | Interface web do Airflow (login `admin`/`admin`) |

As DAGs da pasta `aula_04/code/dags/` são **carregadas automaticamente** no boot do container do Airflow — incluindo a `dataflow_vendas_diarias`.

> **⚠️ Importante:** o AWS Academy Learner Lab só permite as regiões `us-east-1` e `us-west-2`, não deixa criar usuários/roles IAM (por isso reusamos a role `LabRole`) e as credenciais expiram quando a sessão do lab encerra (~4h). Este ambiente é **efêmero e didático**.

---

## Pré-requisitos

| Requisito | Como obter |
|-----------|------------|
| Conta AWS Academy Learner Lab | Fornecida pelo professor no AWS Academy |
| Repositório do curso | `git clone` do repositório em qualquer máquina/terminal |
| Terraform e AWS CLI | Instalados nos Passos 2 e 3 abaixo |

> **Dica:** Você pode rodar tudo a partir de qualquer terminal Linux/Mac com acesso à internet (incluindo o próprio terminal do AWS CloudShell). Os passos abaixo assumem um terminal Linux.

> **⚠️ AWS CloudShell:** o home (`~`) do CloudShell tem apenas **1 GB**, e o provider AWS ocupa ~700 MB. Antes do `terraform init`, aponte o cache do Terraform para `/tmp` (mais espaço): `export TF_PLUGIN_CACHE_DIR=/tmp/tf-plugin-cache && export TF_DATA_DIR=/tmp/tf-data && mkdir -p /tmp/tf-plugin-cache`. Veja o troubleshooting no fim do arquivo.

---

## Passo 1: Iniciar o Lab e Copiar as Credenciais

**Descrição:** Ligar o Learner Lab e pegar as credenciais temporárias da AWS.

**Passos no AWS Academy:**

1. Entre no curso no AWS Academy e clique em **Start Lab**.
2. Aguarde o indicador ao lado de "AWS" ficar **verde** (leva ~1 min).
3. Clique em **AWS Details** e, em **AWS CLI**, clique em **Show**.
4. Copie o bloco de credenciais exibido (algo como `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`).

> **⚠️ Nunca** cole essas credenciais em chats, e-mails ou commits. Elas dão acesso à sua conta do lab enquanto a sessão estiver ativa.

---

## Passo 2: Instalar o Terraform no Terminal

**Descrição:** Instalar o Terraform. Escolha a opção conforme o seu terminal.

### Opção A — AWS CloudShell ou Amazon Linux / RHEL

**Comando:**
```bash
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo
sudo yum -y install terraform
```

### Opção B — Ubuntu / Debian

**Comando:**
```bash
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common curl
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform
```

### Opção C — Sem permissão de root (binário no seu diretório)

**Comando:**
```bash
cd ~
TF_VERSION=1.9.8
curl -fsSLO "https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_amd64.zip"
unzip -o "terraform_${TF_VERSION}_linux_amd64.zip"
mkdir -p ~/bin && mv terraform ~/bin/
export PATH="$HOME/bin:$PATH"
```

**Verificação (qualquer opção):**
```bash
terraform version
```

**Resultado esperado:**
```
Terraform v1.9.x
on linux_amd64
```

**Explicação:** O Terraform lê os arquivos `.tf` da pasta `infra/` deste repositório e cria os recursos na AWS. É a mesma ferramenta usada em produção para versionar infraestrutura.

---

## Passo 3: Instalar/Confirmar a AWS CLI

**Descrição:** A AWS CLI é usada para autenticar e para descobrir as URLs dos serviços depois.

> **Dica:** No **AWS CloudShell** a AWS CLI já vem instalada — pule para a verificação.

**Comando (instalar, se necessário):**
```bash
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip -o awscliv2.zip
sudo ./aws/install --update || ./aws/install -i ~/aws-cli -b ~/bin
```

**Verificação:**
```bash
aws --version
```

**Resultado esperado:**
```
aws-cli/2.x.x Python/3.x.x Linux/...
```

---

## Passo 4: Configurar as Credenciais do Lab

**Descrição:** Exportar as credenciais copiadas no Passo 1 como variáveis de ambiente.

**Comando (substitua pelos valores do seu lab):**
```bash
export AWS_ACCESS_KEY_ID="SEU_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="SUA_SECRET_ACCESS_KEY"
export AWS_SESSION_TOKEN="SEU_SESSION_TOKEN"
export AWS_DEFAULT_REGION="us-east-1"
```

**Verificação:**
```bash
aws sts get-caller-identity
```

**Resultado esperado:**
```json
{
    "UserId": "...:user...",
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/voclabs/user..."
}
```

**Explicação:** Essas credenciais são temporárias. Se a sessão do lab expirar ou você clicar em **End Lab**, precisará copiá-las de novo (Passo 1) e reexportar.

> **⚠️ Região:** confira no início do seu `session_token`/painel se a sessão é `us-east-1` ou `us-west-2` e use a mesma em `AWS_DEFAULT_REGION`. As duas são permitidas no Learner Lab.

---

## Passo 5: Clonar o Repositório e Entrar na Pasta `infra/`

**Descrição:** Obter o código da infraestrutura.

**Comando:**
```bash
git clone https://github.com/AleTavares/Mackenzie_BigDataProcessing.git
cd Mackenzie_BigDataProcessing/infra
```

> Se você já tem o repositório, apenas `cd` até a pasta `infra/`.

---

## Passo 6: Habilitar o Airflow e Ajustar Variáveis

**Descrição:** Criar um arquivo `terraform.tfvars` habilitando o Airflow (Aula 4).

**Comando:**
```bash
cp terraform.tfvars.example terraform.tfvars
```

Edite o `terraform.tfvars` e garanta a linha:
```hcl
enable_airflow = true
```

> **Dica de segurança:** por padrão o acesso fica aberto (`allowed_cidr = "0.0.0.0/0"`). Se quiser restringir ao IP da faculdade, ajuste `allowed_cidr` para algo como `"203.0.113.10/32"`.

---

## Passo 7: Subir o Ambiente com Terraform

**Descrição:** Provisionar o cluster ECS, o Jupyter e o Airflow no Fargate.

**Comando:**
```bash
terraform init
terraform apply -auto-approve
```

**Resultado esperado (final):**
```
Apply complete! Resources: 10 added, 0 changed, 0 destroyed.
```

**Explicação:** O Terraform cria o cluster ECS, as definições de task, os services Fargate e os security groups. Na primeira vez, o Fargate baixa as imagens (Jupyter ~2 GB, Airflow ~400 MB), então as tasks levam **1 a 3 minutos** para ficarem prontas.

> **⚠️ Importante:** o Terraform guarda o estado localmente (arquivo `terraform.tfstate`). Rode os comandos sempre a partir da **mesma pasta** para conseguir derrubar o ambiente depois.

---

## Passo 8: Descobrir as URLs de Acesso

**Descrição:** Obter os IPs públicos do Jupyter e do Airflow.

**Comando:**
```bash
./get_urls.sh
```

**Resultado esperado:**
```
Cluster: dataflow-lab-cluster | Região: us-east-1

✅ Jupyter:  http://<IP_PUBLICO>:8888  (sem token)
✅ Airflow:  http://<IP_PUBLICO>:8081  (login: admin/admin)
   Obs.: a UI leva ~1-2 min extras para responder (db init + start).
```

**Explicação:** Cada task Fargate recebe um IP público próprio. O script consulta a AWS e monta as URLs. Se aparecer "ainda sem IP público", aguarde ~1 min e rode de novo.

> **⚠️ Os IPs mudam** a cada vez que uma task é recriada. Rode `./get_urls.sh` novamente sempre que precisar.

---

## Passo 9: Acessar o Airflow e Validar a DAG

**Descrição:** Abrir a UI do Airflow e confirmar que a DAG da aula está carregada.

**Passos no navegador:**

1. Abra a URL do Airflow (porta **8081**) exibida no passo anterior.
2. Faça login com **admin / admin**.
3. Na lista de DAGs, localize a DAG **`dataflow_vendas_diarias`**.

**Resultado esperado:**
- A DAG `dataflow_vendas_diarias` aparece na lista (pode levar até ~30s após o webserver subir).
- O status do Scheduler no topo aparece como "healthy".

**Explicação:** Os arquivos `.py` de `aula_04/code/dags/` são gravados dentro do container no boot. Assim, a DAG da aula já vem pronta — sem precisar montar volumes como no ambiente Docker local.

> **Carlos:** "Repare que aqui usamos `SequentialExecutor` com SQLite — a combinação suportada para um único container. É perfeita para o lab: uma task por vez, mas com todos os conceitos de orquestração que você precisa aprender."

---

## Passo 10: Derrubar o Ambiente (IMPORTANTE)

**Descrição:** Destruir todos os recursos para não consumir o crédito do lab.

**Comando:**
```bash
terraform destroy -auto-approve
```

**Resultado esperado:**
```
Destroy complete! Resources: 10 destroyed.
```

**Explicação:** Sempre rode o `destroy` ao terminar. Se você apenas fechar o navegador, os recursos continuam rodando e consumindo crédito. Ao clicar em **End Lab** no AWS Academy, a AWS pode limpar recursos automaticamente, mas o `terraform destroy` é a forma limpa e recomendada.

> **⚠️ Seus notebooks e alterações são efêmeros.** Baixe qualquer arquivo importante do Jupyter antes de derrubar. As DAGs versionadas no repositório são recarregadas a cada `apply`, então essas não se perdem.

---

## Troubleshooting

### Problema: `terraform` não é reconhecido após instalar

**Solução:**
```bash
# Se instalou o binário no ~/bin, garanta que está no PATH:
export PATH="$HOME/bin:$PATH"
terraform version
```

---

### Problema: `no space left on device` no `terraform init`

**Sintoma:** o `terraform init` falha ao instalar o provider AWS com `Error while installing hashicorp/aws ...: no space left on device`.

**Causa:** o provider da AWS ocupa ~700 MB. No **AWS CloudShell** o diretório home (`~`) tem cota de apenas **1 GB**, que estoura ao somar o provider com o repositório clonado.

**Solução (recomendada) — mandar o cache do Terraform para `/tmp`:**
```bash
# /tmp no CloudShell é efêmero mas tem mais espaço que o home
export TF_PLUGIN_CACHE_DIR=/tmp/tf-plugin-cache
export TF_DATA_DIR=/tmp/tf-data
mkdir -p "$TF_PLUGIN_CACHE_DIR"

# limpe um init anterior que tenha ficado pela metade
rm -rf .terraform
terraform init
```

> **Atenção:** como `/tmp` é efêmero no CloudShell, se a sessão reiniciar você precisa reexportar essas variáveis e rodar `terraform init` de novo antes de `apply`/`destroy`.

**Solução alternativa — liberar espaço no home:**
```bash
rm -rf ~/.cache/* 2>/dev/null
df -h ~          # confira o espaço livre
```

> **Dica:** se possível, rode o Terraform de uma máquina/terminal com mais disco (não o CloudShell) para evitar o limite de 1 GB.

---

### Problema: `ExpiredToken` ou `InvalidClientTokenId`

**Sintoma:** `aws sts get-caller-identity` retorna erro de token expirado.

**Solução:** As credenciais do lab expiraram. Volte ao **Passo 1**, copie as credenciais novas em **AWS Details** e reexporte as variáveis (Passo 4).

---

### Problema: `terraform apply` falha com região não permitida

**Sintoma:** Erro de validação da variável `aws_region`.

**Solução:** O Learner Lab só aceita `us-east-1` ou `us-west-2`. Ajuste `AWS_DEFAULT_REGION` e rode com a mesma região:
```bash
export AWS_DEFAULT_REGION="us-east-1"
terraform apply -auto-approve -var aws_region=us-east-1
```

---

### Problema: `get_urls.sh` diz "ainda sem IP público"

**Solução:** As tasks ainda estão iniciando (baixando imagem). Aguarde ~1-2 min e rode `./get_urls.sh` novamente.

---

### Problema: A UI do Airflow demora a responder

**Sintoma:** A URL do Airflow não abre logo após o `apply`.

**Solução:** É esperado. O container faz `db migrate` + criação do admin antes de servir a UI — pode levar ~1-2 min a mais que o Jupyter. Aguarde e recarregue.

---

## Checklist de Validação

- [ ] `terraform version` e `aws --version` funcionam
- [ ] `aws sts get-caller-identity` retorna sua conta do lab
- [ ] `terraform apply` terminou com "Apply complete! Resources: 10 added"
- [ ] `./get_urls.sh` exibe as URLs de Jupyter e Airflow
- [ ] Jupyter abre em `http://<IP>:8888` (sem token)
- [ ] Airflow abre em `http://<IP>:8081` (login admin/admin)
- [ ] A DAG `dataflow_vendas_diarias` aparece na UI do Airflow
- [ ] Ao terminar, `terraform destroy` removeu todos os recursos

---

## Resumo dos Comandos

```bash
# 1. Credenciais (copie do AWS Academy > AWS Details)
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."
export AWS_DEFAULT_REGION="us-east-1"

# 2. Subir
cd Mackenzie_BigDataProcessing/infra
cp terraform.tfvars.example terraform.tfvars   # enable_airflow = true
terraform init
terraform apply -auto-approve

# 3. URLs
./get_urls.sh

# 4. Derrubar ao terminar
terraform destroy -auto-approve
```

Detalhes completos da infraestrutura estão em [`infra/README.md`](../../infra/README.md).
