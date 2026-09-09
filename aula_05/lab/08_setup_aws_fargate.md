# Lab Setup (Alternativo) — Aula 5 no AWS Fargate (AWS Academy)

## Quando usar este guia

⏱️ ~20 minutos (na primeira vez)

Use este caminho **em vez do `00_setup.md`** quando você **não conseguir rodar o Docker localmente** (bloqueio de rede/máquina na faculdade, pouca RAM, etc.). Aqui subimos o ambiente da Aula 5 — **Jupyter + PySpark e o Airflow com as DAGs da Aula 5** — no **AWS ECS Fargate**, dentro do **AWS Academy Learner Lab**, e acessamos tudo pelo navegador.

> **Carlos Mendes (Engenheiro de Dados Sênior):** "A Aula 5 é sobre deixar o pipeline inteligente: branching, sensors e integração com Spark. Se o Docker não sobe na sua máquina, subimos o mesmo Airflow na nuvem — com as três DAGs da aula já carregadas."

> **Marina Silva (CTO):** "Mesmo padrão da aula passada: `terraform apply` sobe, `terraform destroy` derruba. A diferença é a pasta de DAGs — agora apontamos para as da Aula 5."

---

## O que este ambiente sobe

| Serviço | Porta | Função |
|---------|-------|--------|
| Jupyter Notebook + PySpark | 8888 | Interface interativa (Spark em modo `local[*]`) |
| **Airflow UI** | **8081** | Interface web do Airflow (login `admin`/`admin`) |

As DAGs da pasta `aula_05/code/dags/` são **carregadas automaticamente** no boot do container do Airflow:

- `dag_branching_processamento` — BranchPythonOperator
- `dag_sensor_arquivo` (`dataflow_sensor_arquivo_v1`) — FileSensor
- `dag_taskgroups_multi_fonte` — TaskGroups

> **⚠️ Sobre o Exercício 4 (SparkSubmitOperator):** esse exercício submete jobs a um **cluster Spark separado** (`spark://spark-master:7077`). A imagem base do Airflow não inclui o `spark-submit` nem sobe um Spark Master, então **esse exercício específico não roda neste ambiente de container único**. Para o Exercício 4, use o Docker local (`aula_04/lab/00_setup.md`). Os Exercícios 1, 2, 3 e 5 funcionam normalmente aqui.

> **⚠️ Restrições do Learner Lab:** só as regiões `us-east-1` e `us-west-2`, sem criar roles IAM (reusamos a `LabRole`), e as credenciais expiram ao encerrar a sessão (~4h). Ambiente **efêmero e didático**.

---

## Pré-requisitos

| Requisito | Como obter |
|-----------|------------|
| Conta AWS Academy Learner Lab | Fornecida pelo professor no AWS Academy |
| Repositório do curso | `git clone` do repositório |
| Terraform e AWS CLI | Instalados nos Passos 2 e 3 abaixo |

> **Dica:** dá para rodar do próprio **AWS CloudShell** (Terraform via binário e AWS CLI já vem instalada).

> **⚠️ AWS CloudShell:** o home (`~`) tem só **1 GB** e o provider AWS ocupa ~700 MB. Antes do `terraform init`, mande o cache para `/tmp`: `export TF_PLUGIN_CACHE_DIR=/tmp/tf-plugin-cache && export TF_DATA_DIR=/tmp/tf-data && mkdir -p /tmp/tf-plugin-cache`. Veja o troubleshooting no fim do arquivo.

---

## Passo 1: Iniciar o Lab e Copiar as Credenciais

1. No AWS Academy, clique em **Start Lab** e aguarde o indicador "AWS" ficar **verde**.
2. Clique em **AWS Details → AWS CLI: Show** e copie o bloco de credenciais.

> **⚠️ Nunca** cole essas credenciais em chats, e-mails ou commits.

---

## Passo 2: Instalar o Terraform no Terminal

Escolha a opção conforme o seu terminal.

### Opção A — AWS CloudShell ou Amazon Linux / RHEL
```bash
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo
sudo yum -y install terraform
```

### Opção B — Ubuntu / Debian
```bash
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common curl
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform
```

### Opção C — Sem permissão de root (binário no seu diretório)
```bash
cd ~
TF_VERSION=1.9.8
curl -fsSLO "https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_amd64.zip"
unzip -o "terraform_${TF_VERSION}_linux_amd64.zip"
mkdir -p ~/bin && mv terraform ~/bin/
export PATH="$HOME/bin:$PATH"
```

**Verificação:**
```bash
terraform version
```

---

## Passo 3: Instalar/Confirmar a AWS CLI

> No **AWS CloudShell** a AWS CLI já vem instalada — pule para a verificação.

```bash
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip -o awscliv2.zip
sudo ./aws/install --update || ./aws/install -i ~/aws-cli -b ~/bin
```

**Verificação:**
```bash
aws --version
```

---

## Passo 4: Configurar as Credenciais do Lab

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

> **⚠️ Região:** use em `AWS_DEFAULT_REGION` a mesma região da sua sessão (`us-east-1` ou `us-west-2`).

---

## Passo 5: Clonar o Repositório e Entrar na Pasta `infra/`

```bash
git clone https://github.com/AleTavares/Mackenzie_BigDataProcessing.git
cd Mackenzie_BigDataProcessing/infra
```

> Se já tem o repositório, apenas `cd` até a pasta `infra/`.

---

## Passo 6: Habilitar o Airflow e Apontar para as DAGs da Aula 5

**Descrição:** Criar o `terraform.tfvars` habilitando o Airflow **e** apontando a pasta de DAGs para a Aula 5.

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edite o `terraform.tfvars` e garanta estas linhas:
```hcl
enable_airflow  = true
dags_source_dir = "../aula_05/code/dags"
```

> É essa segunda linha que muda em relação à Aula 4 — ela faz o container carregar as DAGs de branching, sensor e taskgroups.

---

## Passo 7: Subir o Ambiente com Terraform

```bash
terraform init
terraform apply -auto-approve
```

**Resultado esperado (final):**
```
Apply complete! Resources: 10 added, 0 changed, 0 destroyed.
```

**Explicação:** o Fargate baixa as imagens na primeira vez (Jupyter ~2 GB, Airflow ~400 MB), então as tasks levam **1 a 3 minutos** para ficarem prontas.

> **⚠️ State local:** rode os comandos sempre da **mesma pasta** para conseguir derrubar depois.

---

## Passo 8: Descobrir as URLs de Acesso

```bash
./get_urls.sh
```

**Resultado esperado:**
```
✅ Jupyter:  http://<IP_PUBLICO>:8888  (sem token)
✅ Airflow:  http://<IP_PUBLICO>:8081  (login: admin/admin)
```

> Os IPs mudam quando uma task é recriada — rode `./get_urls.sh` de novo quando precisar.

---

## Passo 9: Acessar o Airflow e Validar as DAGs

1. Abra a URL do Airflow (porta **8081**) e faça login com **admin / admin**.
2. Confirme que aparecem as DAGs da Aula 5:
   - `dag_branching_processamento`
   - `dataflow_sensor_arquivo_v1`
   - `dag_taskgroups_multi_fonte`

> Pode levar até ~30s após o webserver subir para as DAGs aparecerem.

---

## Passo 10: Preparar o Diretório do FileSensor (Exercício 2)

**Descrição:** A DAG do FileSensor monitora `/opt/airflow/data/incoming/`. Neste ambiente usamos **ECS Exec** para entrar no container e criar o diretório e o arquivo de teste.

**1. Habilitar o ECS Exec no service (uma vez):**
```bash
aws ecs update-service --cluster dataflow-lab-cluster \
  --service dataflow-lab-airflow-svc --enable-execute-command --force-new-deployment
```
Aguarde ~1-2 min a nova task subir (`./get_urls.sh` para reconfirmar o IP).

**2. Descobrir o ID da task e abrir um shell no container:**
```bash
TASK=$(aws ecs list-tasks --cluster dataflow-lab-cluster \
  --service-name dataflow-lab-airflow-svc --query 'taskArns[0]' --output text)

aws ecs execute-command --cluster dataflow-lab-cluster \
  --task "$TASK" --container airflow --interactive --command "/bin/bash"
```

**3. Dentro do container, criar o diretório e simular a chegada do arquivo:**
```bash
mkdir -p /opt/airflow/data/incoming
# o FileSensor espera vendas_<AAAAMMDD>.csv referente à data de execução
echo "order_id,amount,date" > /opt/airflow/data/incoming/vendas_20240115.csv
ls -la /opt/airflow/data/incoming/
exit
```

> **Nota:** se o `execute-command` falhar, é porque o SSM agent ainda não subiu na task nova — aguarde mais um pouco e tente de novo. Alternativamente, você pode ajustar a DAG para apontar o `filepath` a um caminho já existente durante o teste.

**Explicação:** no Docker local você usaria `docker exec` (ver `00_setup.md`); no Fargate o equivalente é o `aws ecs execute-command`.

---

## Passo 11: Derrubar o Ambiente (IMPORTANTE)

```bash
terraform destroy -auto-approve
```

**Resultado esperado:**
```
Destroy complete! Resources: 10 destroyed.
```

> Sempre rode o `destroy` ao terminar para não consumir crédito do lab. Baixe qualquer arquivo importante antes — o container é efêmero.

---

## Sobre o Exercício 4 (SparkSubmitOperator)

O Exercício 4 depende de um **cluster Spark separado** (`spark-master:7077`) e do binário `spark-submit` na imagem do Airflow — nenhum dos dois existe neste ambiente de container único no Fargate. Opções:

1. **Recomendado:** fazer o Exercício 4 no **Docker local** (`aula_04/lab/00_setup.md`), que sobe o Spark Master/Worker junto do Airflow.
2. Estudar o conceito pela leitura do exercício e da DAG de exemplo, e aplicar os demais exercícios (1, 2, 3, 5) aqui no Fargate.

> **Carlos:** "Faz parte do aprendizado entender os limites de cada ambiente. Um container único é ótimo para orquestração leve; processamento distribuído de verdade pede um cluster — e é exatamente esse o ponto do Exercício 4."

---

## Troubleshooting

### `terraform` não é reconhecido após instalar
```bash
export PATH="$HOME/bin:$PATH"
terraform version
```

### `no space left on device` no `terraform init`
O provider AWS ocupa ~700 MB e o **AWS CloudShell** só tem 1 GB no home. Mande o cache do Terraform para `/tmp`:
```bash
export TF_PLUGIN_CACHE_DIR=/tmp/tf-plugin-cache
export TF_DATA_DIR=/tmp/tf-data
mkdir -p "$TF_PLUGIN_CACHE_DIR"
rm -rf .terraform
terraform init
```
> `/tmp` é efêmero no CloudShell: se a sessão reiniciar, reexporte as variáveis antes de `apply`/`destroy`. Se puder, rode de um terminal com mais disco (não o CloudShell).

### `ExpiredToken` / `InvalidClientTokenId`
As credenciais do lab expiraram. Repita o **Passo 1** e reexporte as variáveis (Passo 4).

### `terraform apply` falha com região não permitida
```bash
export AWS_DEFAULT_REGION="us-east-1"
terraform apply -auto-approve -var aws_region=us-east-1 -var dags_source_dir=../aula_05/code/dags
```

### As DAGs da Aula 5 não aparecem
Confirme que o `terraform.tfvars` tem `dags_source_dir = "../aula_05/code/dags"` e reaplique. Aguarde ~30s após o webserver subir.

### `get_urls.sh` diz "ainda sem IP público"
As tasks ainda estão iniciando. Aguarde ~1-2 min e rode de novo.

---

## Checklist de Validação

- [ ] `terraform version` e `aws --version` funcionam
- [ ] `aws sts get-caller-identity` retorna sua conta do lab
- [ ] `terraform.tfvars` com `enable_airflow = true` e `dags_source_dir = "../aula_05/code/dags"`
- [ ] `terraform apply` terminou com "Apply complete! Resources: 10 added"
- [ ] `./get_urls.sh` exibe as URLs de Jupyter e Airflow
- [ ] As 3 DAGs da Aula 5 aparecem na UI do Airflow
- [ ] Diretório `/opt/airflow/data/incoming/` criado (para o FileSensor)
- [ ] Ao terminar, `terraform destroy` removeu todos os recursos

---

## Resumo dos Comandos

```bash
# 1. Credenciais (copie do AWS Academy > AWS Details)
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."
export AWS_DEFAULT_REGION="us-east-1"

# 2. Subir apontando para as DAGs da Aula 5
cd Mackenzie_BigDataProcessing/infra
cp terraform.tfvars.example terraform.tfvars
# no tfvars:  enable_airflow = true  e  dags_source_dir = "../aula_05/code/dags"
terraform init
terraform apply -auto-approve

# 3. URLs
./get_urls.sh

# 4. Derrubar ao terminar
terraform destroy -auto-approve
```

Detalhes completos da infraestrutura estão em [`infra/README.md`](../../infra/README.md).
