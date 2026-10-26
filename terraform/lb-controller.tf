# AWS LB Controller 용 IAM 역할 — 수정 4-① (#54). Service(type: LoadBalancer)·Ingress·Gateway 를 보고 AWS 로드밸런서(NLB·ALB)를 만든다.
# 구조는 ebs-csi.tf 와 같은 IRSA. 다른 점은 정책 — AWS 관리형이 없어 공식 저장소 JSON 을 버전 고정해 가져왔다.
#   출처: kubernetes-sigs/aws-load-balancer-controller v3.6.0 docs/install/iam_policy.json

data "aws_iam_policy_document" "lb_controller_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:sub"
      values   = ["system:serviceaccount:kube-system:aws-load-balancer-controller"] # Helm 차트 기본 SA. 이름만 쓰면 토큰의 sub 와 영원히 불일치
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lb_controller" {
  name               = "${var.project}-lb-controller"
  assume_role_policy = data.aws_iam_policy_document.lb_controller_assume.json
}

# 고객 관리형 정책 — AWS 관리형이 없어 우리가 직접 만든다
resource "aws_iam_policy" "lb_controller" {
  name   = "${var.project}-lb-controller"
  policy = file("${path.module}/policies/lb-controller-v3.6.0.json")
  # 파일 이름의 버전 = 설치할 Helm 차트의 컨트롤러 버전(②). 컨트롤러를 올리면 새 권한이 필요할 수 있어 정책도 같이 갈아야 한다
  #   — 버전이 안 보이면 "컨트롤러는 새 버전, 정책은 옛 버전"이 돼 AccessDenied 가 나도 원인을 못 찾는다.
}

resource "aws_iam_role_policy_attachment" "lb_controller" {
  role       = aws_iam_role.lb_controller.name
  policy_arn = aws_iam_policy.lb_controller.arn # ebs-csi.tf 의 관리형 정책은 AWS 에 이미 있어 ARN 이 고정 문자열.
  #                                               이 정책은 apply 때 생겨서 ARN 을 미리 모름 → 참조(생성 순서도 Terraform 이 정함)
}

# 수정 4-② (#56) 컨트롤러 설치 — EKS 애드온 목록에 없어서 Helm 차트로 설치한다.
resource "helm_release" "lb_controller" {
  name       = "aws-load-balancer-controller"
  repository = "https://aws.github.io/eks-charts"
  chart      = "aws-load-balancer-controller"
  version    = "3.6.0"       # policies/lb-controller-v3.6.0.json 과 짝. 올릴 땐 정책 JSON 도 같이
  namespace  = "kube-system" # sub = system:serviceaccount:<여기>:<SA 이름>

  values = [yamlencode({
    clusterName = module.eks.cluster_name
    region      = var.region
    vpcId       = module.vpc.vpc_id
    serviceAccount = {
      create = true
      name   = "aws-load-balancer-controller" # sub 의 맨 끝 조각만 — 앞부분은 k8s 가 붙인다
      annotations = {
        "eks.amazonaws.com/role-arn" = aws_iam_role.lb_controller.arn # 정책(aws_iam_policy) ARN 과 헷갈리지 말 것
        # 없으면 EKS 웹훅이 토큰을 주입하지 않아 파드가 노드 역할로 AWS 를 호출 → ELB 권한이 없어 AccessDenied
      }
    }
  })]
}
