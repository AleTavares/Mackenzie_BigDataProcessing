# =============================================================================
# Configuração do Provider AWS
# =============================================================================
# No AWS Academy Learner Lab, NÃO usamos "assume_role": as credenciais
# temporárias (aws_access_key_id / aws_secret_access_key / aws_session_token)
# fornecidas na página "AWS Details" do lab já são as credenciais de trabalho.
#
# Configure-as antes de rodar o Terraform, por exemplo:
#
#   export AWS_ACCESS_KEY_ID="..."
#   export AWS_SECRET_ACCESS_KEY="..."
#   export AWS_SESSION_TOKEN="..."
#   export AWS_DEFAULT_REGION="us-east-1"
#
# Ou cole o bloco de credenciais em ~/.aws/credentials (perfil default).
# =============================================================================

provider "aws" {
  region = var.aws_region

  # Tags padrão aplicadas a todos os recursos que suportam tagging.
  default_tags {
    tags = {
      Project   = "mackenzie-bigdata-processing"
      ManagedBy = "terraform"
      Env       = "aws-academy-lab"
    }
  }
}
