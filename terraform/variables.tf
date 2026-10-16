variable "region" {
  description = "서울 리전. ECR 저장소도 여기 있다."
  type        = string
  default     = "ap-northeast-2"
}

variable "project" {
  description = "비용 태그와 리소스 이름 접두어"
  type        = string
  default     = "ticket-eks"
}

variable "cluster_version" {
  description = <<-EOT
    EKS 1.36. 최신(1.37)보다 한 단계 낮게 만들어 4단계에서 1.37 로 한 번 올린다(로드맵 4-1).
    표준 지원이 2027-08-02 까지라 작업 시기(2027 1~3월)에 여유가 있다.
    1.35 는 2027-03-27 에 표준 지원이 끝나 작업이 밀리면 확장 지원(요금 6배)으로 떨어진다.
  EOT
  type        = string
  default     = "1.36"
}
