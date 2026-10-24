# EBS CSI 드라이버용 IAM 역할 — 2단계 수정 2 (#51). PVC 가 EBS 볼륨을 받으려면 드라이버가 AWS API 를 불러야 한다.
#
# IRSA 를 고른 이유: 노드 역할에 붙이면 노드의 모든 파드가 볼륨 생성·삭제 권한을 갖는다(최소 권한 위반).
#   Pod Identity(2023~, 새 클러스터 권장)도 있지만 IRSA 는 OIDC·토큰·신뢰 정책을 직접 써 보게 되고 기존 클러스터에 더 흔하다.
#   OIDC 공급자는 EKS 모듈이 enable_irsa(기본 true)로 이미 만든다.

data "aws_iam_policy_document" "ebs_csi_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn] # 이 클러스터의 OIDC 공급자가 발급한 토큰만
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:sub"
      values   = ["system:serviceaccount:kube-system:ebs-csi-controller-sa"]
      # 드라이버 컨트롤러의 SA 만. 없으면 클러스터의 모든 SA 가 이 역할을 받아 EBS 볼륨을 만들고 지울 수 있다(최소 권한 위반).
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
      # STS 에 내라고 발급된 토큰만 — 다른 용도로 발급된 토큰의 재사용을 막는다.
      # ⚠️ 완전 일치 비교라 오타가 나도 validate·plan 은 통과하고, apply 후 AccessDenied → PVC Pending 으로만 드러난다.
    }
  }
}

resource "aws_iam_role" "ebs_csi" {
  name               = "${var.project}-ebs-csi"
  assume_role_policy = data.aws_iam_policy_document.ebs_csi_assume.json
}

# AWS 관리형 정책 — ec2:CreateVolume·AttachVolume·DeleteVolume 등 드라이버에 필요한 것만 묶여 있다.
resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}
