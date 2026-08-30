# =============================================================================
# Variáveis de entrada
# =============================================================================

variable "aws_region" {
  description = "Região AWS. No AWS Academy Learner Lab apenas us-east-1 e us-west-2 são permitidas."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = contains(["us-east-1", "us-west-2"], var.aws_region)
    error_message = "O AWS Academy Learner Lab só permite us-east-1 ou us-west-2."
  }
}

variable "project_name" {
  description = "Prefixo usado no nome dos recursos criados."
  type        = string
  default     = "dataflow-lab"
}

variable "lab_role_name" {
  description = "Nome da role pré-existente do AWS Academy usada como execution e task role. No Learner Lab é sempre 'LabRole' (não é possível criar roles IAM)."
  type        = string
  default     = "LabRole"
}

variable "jupyter_image" {
  description = "Imagem de container do Jupyter com PySpark embutido (mesma usada no docker-compose base)."
  type        = string
  default     = "quay.io/jupyter/pyspark-notebook:latest"
}

variable "task_cpu" {
  description = "CPU da task Fargate em unidades (1024 = 1 vCPU)."
  type        = number
  default     = 2048
}

variable "task_memory" {
  description = "Memória da task Fargate em MiB. Deve ser compatível com task_cpu (Fargate)."
  type        = number
  default     = 8192
}

variable "jupyter_port" {
  description = "Porta em que o Jupyter Notebook escuta e é exposta."
  type        = number
  default     = 8888
}

variable "allowed_cidr" {
  description = "CIDR autorizado a acessar o Jupyter. O padrão 0.0.0.0/0 abre para toda a internet — restrinja ao IP da faculdade em produção."
  type        = string
  default     = "0.0.0.0/0"
}

variable "desired_count" {
  description = "Número de tasks Jupyter em execução."
  type        = number
  default     = 1
}

# ---------------------------------------------------------------------------
# Airflow (Aula 4) - opcional
# ---------------------------------------------------------------------------
variable "enable_airflow" {
  description = "Se true, sobe também o ambiente Airflow (Webserver + Scheduler) para a Aula 4."
  type        = bool
  default     = false
}

variable "airflow_image" {
  description = "Imagem do Apache Airflow. Usa a tag patch-completa 2.8.4 (mesma versão do requirements.txt); a tag '2.8-python3.11' NÃO existe no Docker Hub."
  type        = string
  default     = "apache/airflow:2.8.4-python3.11"
}

variable "airflow_port" {
  description = "Porta externa da UI do Airflow (no lab local é 8081)."
  type        = number
  default     = 8081
}

variable "airflow_task_cpu" {
  description = "CPU da task Fargate do Airflow (1024 = 1 vCPU)."
  type        = number
  default     = 1024
}

variable "airflow_task_memory" {
  description = "Memória da task Fargate do Airflow em MiB (compatível com airflow_task_cpu)."
  type        = number
  default     = 3072
}

variable "airflow_admin_user" {
  description = "Usuário admin criado na UI do Airflow."
  type        = string
  default     = "admin"
}

variable "airflow_admin_password" {
  description = "Senha do admin do Airflow (ambiente de laboratório)."
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "log_retention_days" {
  description = "Dias de retenção dos logs no CloudWatch."
  type        = number
  default     = 7
}
