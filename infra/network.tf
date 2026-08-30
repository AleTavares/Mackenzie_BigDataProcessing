# =============================================================================
# Rede e Segurança
# =============================================================================
# Reutilizamos a VPC default do AWS Academy Learner Lab em vez de criar uma
# nova. Isso reduz a superfície de recursos criados (menos chances de esbarrar
# em limites de permissão) e usa subnets públicas já com Internet Gateway,
# necessárias para o Fargate baixar a imagem e para o acesso público ao Jupyter.
# =============================================================================

# VPC default da conta/região
data "aws_vpc" "default" {
  default = true
}

# Subnets da VPC default (usadas pelo service Fargate)
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ---------------------------------------------------------------------------
# Security Group - libera a porta do Jupyter e todo o tráfego de saída
# ---------------------------------------------------------------------------
resource "aws_security_group" "jupyter" {
  name        = "${var.project_name}-jupyter-sg"
  description = "Permite acesso ao Jupyter Notebook (porta ${var.jupyter_port})"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Jupyter Notebook"
    from_port   = var.jupyter_port
    to_port     = var.jupyter_port
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    description = "Saida liberada (download de imagem, pacotes, etc.)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-jupyter-sg"
  }
}

# ---------------------------------------------------------------------------
# Security Group - Airflow (criado apenas quando enable_airflow = true)
# ---------------------------------------------------------------------------
resource "aws_security_group" "airflow" {
  count       = var.enable_airflow ? 1 : 0
  name        = "${var.project_name}-airflow-sg"
  description = "Permite acesso a UI do Airflow (porta ${var.airflow_port})"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Airflow Webserver UI"
    from_port   = var.airflow_port
    to_port     = var.airflow_port
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    description = "Saida liberada (download de imagem, pacotes, etc.)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-airflow-sg"
  }
}
