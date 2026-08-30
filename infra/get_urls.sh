#!/usr/bin/env bash
# =============================================================================
# get_urls.sh
# Descobre as URLs públicas dos ambientes subidos no Fargate (AWS Academy):
#   - Jupyter (sempre)
#   - Airflow (quando enable_airflow = true)
# Uso: ./get_urls.sh
# =============================================================================
set -euo pipefail

CLUSTER="$(terraform output -raw ecs_cluster_name 2>/dev/null || echo 'dataflow-lab-cluster')"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
JUPYTER_PORT="$(terraform output -raw jupyter_port 2>/dev/null || echo '8888')"
AIRFLOW_ENABLED="$(terraform output -raw airflow_enabled 2>/dev/null || echo 'false')"
AIRFLOW_PORT="$(terraform output -raw airflow_port 2>/dev/null || echo '8081')"

echo "Cluster: $CLUSTER | Região: $REGION"
echo ""

# Resolve o IP público de uma task, dado o nome do service.
public_ip_for_service() {
  local service_name="$1"
  local task_arn eni_id ip

  task_arn="$(aws ecs list-tasks --cluster "$CLUSTER" --service-name "$service_name" \
    --region "$REGION" --query 'taskArns[0]' --output text 2>/dev/null || echo 'None')"

  if [[ -z "$task_arn" || "$task_arn" == "None" ]]; then
    echo ""  # sem task ainda
    return 0
  fi

  eni_id="$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$task_arn" \
    --region "$REGION" \
    --query "tasks[0].attachments[0].details[?name=='networkInterfaceId'].value" \
    --output text 2>/dev/null || echo '')"

  [[ -z "$eni_id" || "$eni_id" == "None" ]] && { echo ""; return 0; }

  ip="$(aws ec2 describe-network-interfaces --network-interface-ids "$eni_id" \
    --region "$REGION" \
    --query "NetworkInterfaces[0].Association.PublicIp" --output text 2>/dev/null || echo '')"

  [[ "$ip" == "None" ]] && ip=""
  echo "$ip"
}

# --- Jupyter ---
JUPYTER_SVC="$(terraform output -raw ecs_service_name 2>/dev/null || echo 'dataflow-lab-jupyter-svc')"
JUPYTER_IP="$(public_ip_for_service "$JUPYTER_SVC")"
if [[ -n "$JUPYTER_IP" ]]; then
  echo "✅ Jupyter:  http://${JUPYTER_IP}:${JUPYTER_PORT}  (sem token)"
else
  echo "⏳ Jupyter ainda sem IP público. Aguarde a task ficar RUNNING e rode de novo."
fi

# --- Airflow (opcional) ---
if [[ "$AIRFLOW_ENABLED" == "true" ]]; then
  AIRFLOW_SVC="$(terraform output -raw airflow_service_name 2>/dev/null || echo 'dataflow-lab-airflow-svc')"
  AIRFLOW_IP="$(public_ip_for_service "$AIRFLOW_SVC")"
  if [[ -n "$AIRFLOW_IP" ]]; then
    echo "✅ Airflow:  http://${AIRFLOW_IP}:${AIRFLOW_PORT}  (login: admin/admin)"
    echo "   Obs.: a UI leva ~1-2 min extras para responder (db init + start)."
  else
    echo "⏳ Airflow ainda sem IP público. Aguarde a task ficar RUNNING e rode de novo."
  fi
fi
