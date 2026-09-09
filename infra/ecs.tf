# =============================================================================
# ECS Fargate - Jupyter + PySpark
# =============================================================================
# Sobe a mesma imagem do ambiente base (docker-compose.yml) como uma task
# Fargate acessível publicamente pela porta do Jupyter.
#
# RESTRIÇÃO AWS ACADEMY: não é possível criar roles IAM. Por isso usamos a role
# pré-existente "LabRole" tanto como execution role (puxar imagem, escrever
# logs) quanto como task role. Buscamos o ARN dela via data source.
# =============================================================================

# Role pré-existente do AWS Academy (não é criada, apenas referenciada)
data "aws_iam_role" "lab_role" {
  name = var.lab_role_name
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group para os logs do container
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "jupyter" {
  name              = "/ecs/${var.project_name}-jupyter"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.project_name}-jupyter-logs"
  }
}

# ---------------------------------------------------------------------------
# Cluster ECS
# ---------------------------------------------------------------------------
resource "aws_ecs_cluster" "this" {
  name = "${var.project_name}-cluster"

  setting {
    name  = "containerInsights"
    value = "disabled"
  }

  tags = {
    Name = "${var.project_name}-cluster"
  }
}

# Habilita o provedor de capacidade FARGATE no cluster
resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
  }
}

# ---------------------------------------------------------------------------
# Task Definition - Jupyter + PySpark (modo local[*], igual ao lab)
# ---------------------------------------------------------------------------
resource "aws_ecs_task_definition" "jupyter" {
  family                   = "${var.project_name}-jupyter"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory

  # Ambas apontam para a LabRole (única role utilizável no Learner Lab)
  execution_role_arn = data.aws_iam_role.lab_role.arn
  task_role_arn      = data.aws_iam_role.lab_role.arn

  container_definitions = jsonencode([
    {
      name      = "jupyter"
      image     = var.jupyter_image
      essential = true

      # No boot: clona o repositório do curso e disponibiliza os notebooks de
      # cada aula e os datasets dentro de /home/jovyan/work (equivalente aos
      # volumes do docker-compose base). Depois sobe o Jupyter sem token.
      # ATENÇÃO: sem token o acesso é aberto a quem alcançar a porta. Restrinja
      # o acesso pelo security group (var.allowed_cidr) em ambientes expostos.
      entryPoint = ["/bin/bash", "-c"]
      command = [
        join(" && ", [
          "cd /home/jovyan/work",
          # clona o repo (usa a saída de internet do Learner Lab)
          "(git clone --depth 1 --branch ${var.repo_branch} ${var.repo_url} /tmp/curso 2>/dev/null || (rm -rf /tmp/curso && git clone --depth 1 ${var.repo_url} /tmp/curso))",
          # notebooks de cada aula -> work/aula_XX
          "for d in /tmp/curso/aula_*/code; do a=$(basename $(dirname $d)); rm -rf /home/jovyan/work/$a; cp -r $d /home/jovyan/work/$a; done",
          # datasets do curso -> work/data
          "if [ -d /tmp/curso/datasets ]; then rm -rf /home/jovyan/work/data; cp -r /tmp/curso/datasets /home/jovyan/work/data; fi",
          "echo '>> notebooks e datasets prontos em /home/jovyan/work'",
          "exec start-notebook.py --IdentityProvider.token='' --ServerApp.ip=0.0.0.0"
        ])
      ]

      portMappings = [
        {
          containerPort = var.jupyter_port
          hostPort      = var.jupyter_port
          protocol      = "tcp"
        }
      ]

      environment = [
        { name = "JUPYTER_TOKEN", value = "" }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.jupyter.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "jupyter"
        }
      }
    }
  ])

  tags = {
    Name = "${var.project_name}-jupyter-task"
  }
}

# ---------------------------------------------------------------------------
# Service ECS - mantém a task rodando com IP público
# ---------------------------------------------------------------------------
resource "aws_ecs_service" "jupyter" {
  name            = "${var.project_name}-jupyter-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.jupyter.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.jupyter.id]
    assign_public_ip = true # necessário na VPC default para puxar imagem e acesso externo
  }

  # Evita corrida entre o service e o provedor de capacidade do cluster
  depends_on = [aws_ecs_cluster_capacity_providers.this]

  tags = {
    Name = "${var.project_name}-jupyter-svc"
  }
}
