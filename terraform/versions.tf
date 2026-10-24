# 버전 제약만 선언한다. 리소스는 1단계부터.
terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0" # 6.x 안에서만 올라간다(6.9 OK, 7.0 X)
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.0" # 3.0 부터 provider 안 kubernetes 가 블록 → 속성(= { })으로 바뀌었다
    }
  }
}
