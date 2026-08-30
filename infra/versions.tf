# =============================================================================
# Terraform e Providers - Infra AWS Fargate (AWS Academy Learner Lab)
# Curso: Big Data Processing - MBA Engenharia de Dados (Mackenzie)
# =============================================================================
# IMPORTANTE (AWS Academy Learner Lab):
#   - O state é LOCAL de propósito. As credenciais do Learner Lab são
#     temporárias e mudam a cada início de sessão, o que dificulta usar
#     backend remoto (S3) com role fixa. Mantemos o state local e simples.
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
