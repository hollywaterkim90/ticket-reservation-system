# EKS 클러스터 — 1단계. 리소스를 직접 쓰면 IAM 역할·보안그룹·OIDC 까지 100줄이 넘는다.
#
# ⚠️ 모듈 v21 부터 변수명에서 `cluster_` 접두어가 빠졌다(cluster_name→name,
#    cluster_version→kubernetes_version, cluster_addons→addons ...).
#    v20 은 aws provider <6.0 만 지원해서 우리 provider(~>6.0)와 충돌한다.
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name               = "${var.project}-cluster"
  kubernetes_version = var.cluster_version # 1.36 (최신 1.37 보다 N-1 → 4단계에서 한 번 올린다)

  # 1단계는 public. 내 맥에서 바로 kubectl 이 되어야 "앱은 뜬다"를 기준선으로 잡을 수 있다.
  # 3b 에서 Public → Public+Private → Private only 로 3단 조인다(그때 bastion 문제가 생긴다).
  endpoint_public_access = true

  # Access Entries 방식(신방식). 옛 aws-auth ConfigMap 과 섞인 모드도 있으나
  # 2-1 에서 두 번째 IAM 사용자에게 권한을 줘볼 것이므로 API 모드로 간다.
  authentication_mode = "API"
  # 클러스터를 만든 주체(admin)를 자동으로 클러스터 관리자로 등록.
  enable_cluster_creator_admin_permissions = true

  vpc_id = module.vpc.vpc_id
  # 노드와 컨트롤플레인 ENI 를 private 서브넷에 둔다. 공인 IP 가 없어 외부에서 직접 접근 불가.
  subnet_ids = module.vpc.private_subnets

  # 기본 애드온 3개 + EBS CSI(IRSA 역할은 ebs-csi.tf). LB Controller 는 수정 4 에서.
  addons = {
    coredns            = {}
    kube-proxy         = {}
    vpc-cni            = {}
    aws-ebs-csi-driver = { service_account_role_arn = aws_iam_role.ebs_csi.arn }
  }

  # 컨트롤플레인 로그(audit·authenticator 등)는 끈다 — CloudWatch 저장 비용이 든다.
  # 필요할 때 켜면 되고, "왜 껐나"를 말할 수 있으면 된다.
  enabled_log_types = []

  eks_managed_node_groups = {
    default = {
      instance_types = ["t3.medium"] # amd64. 이미지도 --platform linux/amd64 로 빌드했다(0-1)
      ami_type       = "AL2023_x86_64_STANDARD"

      # max 를 2로 묶어 비용 사고를 막는다. 노드당 최대 파드 17 / IP 18개.
      min_size     = 1
      desired_size = 2
      max_size     = 2
    }
  }
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "kubeconfig_command" {
  description = "apply 후 이 명령으로 kubectl 연결"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}
