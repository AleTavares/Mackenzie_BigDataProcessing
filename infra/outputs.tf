# =============================================================================
# Outputs
# =============================================================================
# O IP público do Jupyter fica na ENI (interface de rede) da task Fargate, que
# só é criada após o service subir a task. Por isso não expomos a URL direta
# aqui — em vez disso, mostramos os comandos para descobrir o IP após o apply.
# =============================================================================

output "ecs_cluster_name" {
  description = "Nome do cluster ECS criado."
  value       = aws_ecs_cluster.this.name
}

output "ecs_service_name" {
  description = "Nome do service ECS do Jupyter."
  value       = aws_ecs_service.jupyter.name
}

output "jupyter_port" {
  description = "Porta em que o Jupyter é exposto."
  value       = var.jupyter_port
}

output "log_group" {
  description = "CloudWatch Log Group com os logs do container Jupyter."
  value       = aws_cloudwatch_log_group.jupyter.name
}

output "airflow_enabled" {
  description = "Indica se o ambiente Airflow (Aula 4) foi provisionado."
  value       = var.enable_airflow
}

output "airflow_service_name" {
  description = "Nome do service ECS do Airflow (vazio se enable_airflow = false)."
  value       = var.enable_airflow ? aws_ecs_service.airflow[0].name : ""
}

output "airflow_port" {
  description = "Porta em que a UI do Airflow é exposta."
  value       = var.airflow_port
}

output "como_obter_url" {
  description = "Como descobrir as URLs públicas após o apply."
  value       = <<-EOT

    Aguarde ~1-2 min as tasks ficarem RUNNING e rode o atalho:

        ./get_urls.sh

    Ele imprime a URL do Jupyter (porta ${var.jupyter_port})${var.enable_airflow ? " e do Airflow (porta ${var.airflow_port}, login ${var.airflow_admin_user}/****)" : ""}.
  EOT
}
