provider "aws" {
  region = var.region

  # 모든 리소스에 자동으로 붙는 태그.
  # Cost Explorer 에서 Project 태그로 필터하면 이 실습 비용만 분리해서 볼 수 있다 → 글③(비용 실측) 재료.
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

# Helm 은 AWS API 가 아니라 클러스터(k8s API)에 설치한다 → 접속 정보가 따로 필요하다.
# 토큰은 파일에 저장하지 않고 실행할 때마다 aws eks get-token 으로 받는다(15분 만료) — kubeconfig 와 같은 방식.
provider "helm" {
  kubernetes = {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    exec = {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
    }
  }
}
