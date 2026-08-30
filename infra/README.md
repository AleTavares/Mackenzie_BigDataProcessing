# Infra — Ambiente do curso no AWS Fargate (AWS Academy)

Terraform para subir o ambiente do curso em **AWS ECS Fargate**, dentro do **AWS Academy Learner Lab**. É uma alternativa para quando os alunos **não conseguem rodar Docker localmente** (bloqueios de rede/máquina na faculdade): o ambiente sobe na nuvem e é acessado pelo navegador.

Dois ambientes, controlados por variável:

- **Jupyter + PySpark** (sempre) — mesma imagem do ambiente base (`quay.io/jupyter/pyspark-notebook`), Spark em modo `local[*]`. Cobre as **Aulas 1–3**.
- **Apache Airflow** (opcional, `enable_airflow = true`) — Webserver + Scheduler no mesmo container (LocalExecutor + SQLite), igual ao lab local. Cobre a **Aula 4**. As DAGs de `aula_04/code/dags/*.py` são **carregadas automaticamente** no boot do container (sem bucket nem build de imagem).

## Por que este desenho (restrições do AWS Academy Learner Lab)

O Learner Lab é limitado, e o Terraform foi escrito respeitando isso:

- **Não é possível criar roles IAM.** Reutilizamos a role pré-existente `LabRole` como execution role e task role (referenciada via data source, nunca criada).
- **Regiões permitidas:** apenas `us-east-1` e `us-west-2` (há uma validação na variável).
- **Sem `assume_role`.** Usamos direto as credenciais temporárias fornecidas na aba "AWS Details" do lab.
- **State local.** As credenciais mudam a cada sessão do lab, então não usamos backend remoto.
- **VPC default.** Reutilizamos a VPC/subnets default (públicas) em vez de criar rede nova.

## Pré-requisitos

| Item | Como obter |
|------|------------|
| Terraform >= 1.5 | https://developer.hashicorp.com/terraform/install |
| AWS CLI v2 | https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html |
| Sessão AWS Academy Learner Lab ativa | Portal AWS Academy → seu curso → "Start Lab" |

## Passo a passo

### 1. Iniciar o lab e pegar as credenciais

No AWS Academy, clique em **Start Lab** e aguarde o indicador ficar verde. Clique em **AWS Details → AWS CLI: Show** e copie o bloco de credenciais.

Exporte no terminal (elas expiram junto com a sessão do lab, ~4h):

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."
export AWS_DEFAULT_REGION="us-east-1"
```

Confirme que está autenticado:

```bash
aws sts get-caller-identity
```

### 2. Ajustar variáveis (opcional)

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
# edite terraform.tfvars se quiser (ex.: restringir allowed_cidr ao IP da faculdade)
```

Para a **Aula 4** (Airflow), habilite no `terraform.tfvars`:

```hcl
enable_airflow = true
```

### 3. Subir o ambiente

```bash
terraform init
terraform plan
terraform apply
```

### 4. Descobrir as URLs

As tasks levam ~1–2 min para ficar `RUNNING`. O IP público fica na interface de rede de cada task. Use o atalho:

```bash
./get_urls.sh
```

Ele imprime algo como:

```
✅ Jupyter:  http://<IP_PUBLICO>:8888  (sem token)
✅ Airflow:  http://<IP_PUBLICO>:8081  (login: admin/admin)
```

Abra as URLs no navegador. O Jupyter sobe **sem token** (igual ao ambiente local). A UI do Airflow pode levar mais ~1–2 min para responder na primeira vez (inicialização do banco).

### 5. Destruir ao terminar

Para não consumir o crédito do lab, derrube tudo ao final:

```bash
terraform destroy
```

> Se você encerrar o lab pelo portal ("End Lab"), os recursos podem ser removidos automaticamente pela AWS Academy, mas rodar `terraform destroy` é a forma limpa e recomendada.

## Segurança

- Por padrão `allowed_cidr = "0.0.0.0/0"` (aberto à internet) e o Jupyter sobe **sem senha**. Para uso além de teste rápido, **restrinja `allowed_cidr`** ao IP/faixa da faculdade no `terraform.tfvars`.
- Este ambiente é para **fins didáticos** dentro do lab, não para produção.

## Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `versions.tf` | Versão do Terraform e provider AWS |
| `provider.tf` | Configuração do provider (região, tags) |
| `variables.tf` | Variáveis de entrada com validações |
| `network.tf` | VPC/subnets default + security groups (Jupyter e Airflow) |
| `ecs.tf` | Cluster, task definition e service Fargate (Jupyter) |
| `airflow.tf` | Task definition e service Fargate (Airflow) — só com `enable_airflow` |
| `outputs.tf` | Saídas e instruções para obter as URLs |
| `get_urls.sh` | Script que resolve o IP público das tasks (Jupyter e Airflow) |
| `terraform.tfvars.example` | Exemplo de variáveis |

## Limitações conhecidas

- Cobre as **Aulas 1–4** (Jupyter + PySpark local e Airflow). O cluster Spark Master/Worker separado e a stack completa das Aulas 5–7 não estão inclusos; para essas aulas, use o Docker local ou Codespaces.
- No Fargate o Airflow roda **init + webserver + scheduler no mesmo container** com SQLite (cada task tem filesystem próprio). É fiel ao lab, mas não escala para múltiplas réplicas.
- As DAGs de `aula_04/code/dags/*.py` são injetadas no boot (base64 → arquivo). Para adicionar novas DAGs, coloque o `.py` nessa pasta e rode `terraform apply` de novo (recria a task). Esse mecanismo é adequado a arquivos pequenos; para muitas/grandes DAGs, migre para sync via S3.
- O IP público muda a cada nova task; rode `./get_urls.sh` novamente se recriar um service.
- Notebooks e o banco do Airflow ficam no container **efêmero** — baixe seus arquivos antes do `destroy` (não há volume persistente configurado aqui). As DAGs versionadas no repo são recarregadas a cada apply, então não se perdem; DAGs criadas só pela UI, sim.
