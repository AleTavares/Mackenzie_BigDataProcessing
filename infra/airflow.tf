# =============================================================================
# ECS Fargate - Apache Airflow (Aula 4) - opcional
# =============================================================================
# Criado apenas quando enable_airflow = true.
#
# DESENHO: no docker-compose local o Airflow usa 3 containers (init, webserver,
# scheduler) compartilhando um SQLite via volume. No Fargate cada task tem
# filesystem próprio e efêmero, então tasks separadas NÃO compartilhariam o
# SQLite. Para reproduzir fielmente a Aula 4 com o mínimo de recursos, rodamos
# os três passos em UMA ÚNICA task: um container que inicializa o banco, cria o
# admin e sobe webserver + scheduler juntos (LocalExecutor + SQLite).
#
# A UI escuta diretamente na porta var.airflow_port (default 8081), acessível
# pelo IP público da task.
# =============================================================================

# CloudWatch Log Group do Airflow
resource "aws_cloudwatch_log_group" "airflow" {
  count             = var.enable_airflow ? 1 : 0
  name              = "/ecs/${var.project_name}-airflow"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.project_name}-airflow-logs"
  }
}

# ---------------------------------------------------------------------------
# DAGs da Aula 4 carregadas automaticamente
# ---------------------------------------------------------------------------
# Lê todos os arquivos .py da pasta de DAGs do curso e os injeta no container
# no boot (base64 -> arquivo em /opt/airflow/dags). Assim, qualquer DAG que o
# professor adicionar na pasta é carregada no próximo `terraform apply`, sem
# precisar de bucket S3 nem build de imagem.
locals {
  dags_dir = "${path.module}/../aula_04/code/dags"

  # Mapa: nome_do_arquivo => conteúdo em base64
  dag_files = var.enable_airflow ? {
    for f in fileset(local.dags_dir, "*.py") :
    f => base64encode(file("${local.dags_dir}/${f}"))
  } : {}

  # Comandos que recriam cada DAG dentro do container antes de subir o Airflow
  dag_write_cmds = [
    for name, b64 in local.dag_files :
    "echo '${b64}' | base64 -d > /opt/airflow/dags/${name}"
  ]
}

# ---------------------------------------------------------------------------
# Task Definition - Airflow (init + webserver + scheduler no mesmo container)
# ---------------------------------------------------------------------------
resource "aws_ecs_task_definition" "airflow" {
  count                    = var.enable_airflow ? 1 : 0
  family                   = "${var.project_name}-airflow"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.airflow_task_cpu
  memory                   = var.airflow_task_memory

  execution_role_arn = data.aws_iam_role.lab_role.arn
  task_role_arn      = data.aws_iam_role.lab_role.arn

  container_definitions = jsonencode([
    {
      name      = "airflow"
      image     = var.airflow_image
      essential = true

      entryPoint = ["/bin/bash", "-c"]
      # Sequência determinística para SQLite + SequentialExecutor:
      #   1) grava as DAGs da Aula 4
      #   2) migra o schema e ESPERA o banco ficar pronto (db check em loop)
      #   3) cria o admin
      #   4) sobe scheduler (background) e webserver (foreground)
      # Usar 'set -e' só nas fases de init evita que um hiccup do scheduler
      # derrube o webserver.
      command = [
        join("\n", concat(
          [
            "set -e",
            "mkdir -p /opt/airflow/dags",
          ],
          local.dag_write_cmds,
          [
            "echo '>> migrando o banco de metadados...'",
            "airflow db migrate",
            "echo '>> aguardando o banco ficar pronto...'",
            "for i in $(seq 1 30); do airflow db check && break; echo \"  db ainda nao pronto ($i)\"; sleep 2; done",
            "airflow users create --username ${var.airflow_admin_user} --password ${var.airflow_admin_password} --firstname Admin --lastname User --role Admin --email admin@dataflow.local || true",
            "echo '>> iniciando scheduler + webserver...'",
            "set +e",
            "airflow scheduler &",
            "exec airflow webserver --port ${var.airflow_port}"
          ]
        ))
      ]

      portMappings = [
        {
          containerPort = var.airflow_port
          hostPort      = var.airflow_port
          protocol      = "tcp"
        }
      ]

      environment = [
        # SequentialExecutor é a única opção suportada com SQLite (LocalExecutor
        # exige um banco cliente/servidor como Postgres/MySQL). Adequado ao lab.
        { name = "AIRFLOW__CORE__EXECUTOR", value = "SequentialExecutor" },
        { name = "AIRFLOW__CORE__LOAD_EXAMPLES", value = "False" },
        { name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", value = "sqlite:////opt/airflow/airflow.db" },
        { name = "AIRFLOW__WEBSERVER__WEB_SERVER_PORT", value = tostring(var.airflow_port) }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.airflow[0].name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "airflow"
        }
      }
    }
  ])

  tags = {
    Name = "${var.project_name}-airflow-task"
  }
}

# ---------------------------------------------------------------------------
# Service ECS - Airflow
# ---------------------------------------------------------------------------
resource "aws_ecs_service" "airflow" {
  count           = var.enable_airflow ? 1 : 0
  name            = "${var.project_name}-airflow-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.airflow[0].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.airflow[0].id]
    assign_public_ip = true
  }

  depends_on = [aws_ecs_cluster_capacity_providers.this]

  tags = {
    Name = "${var.project_name}-airflow-svc"
  }
}
