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
