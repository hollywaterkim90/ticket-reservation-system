# VPC — 1단계. 공식 모듈을 쓰되 값은 직접 정한다("값이 왜 그런지 설명 가능할 것").
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.project}-vpc"
  cidr = "10.0.0.0/16"

  # EKS 는 최소 2개 AZ(가용영역)를 요구한다. 컨트롤플레인이 두 AZ 에 ENI 를 두고 이중화한다.
  azs = ["ap-northeast-2a", "ap-northeast-2c"]

  # 대역을 용도별 블록으로 띄워 둔다(1~9 public / 11~19 private).
  # 연속 번호(1,2,3,4)로 쓰면 AZ 를 추가할 때 숫자로 용도를 구분할 수 없고,
  # 보안그룹·라우팅 규칙을 블록 단위(10.0.0.0/20 등)로 쓸 수도 없다.
  public_subnets  = ["10.0.1.0/24", "10.0.2.0/24"]
  private_subnets = ["10.0.11.0/24", "10.0.12.0/24"]
  # /24 = 251개 사용 가능. VPC CNI 는 파드에 VPC IP 를 직접 주고 ENI 단위로 선점하므로
  # 운영이라면 /20 이상이 기준. t3.medium 2대(노드당 최대 18 IP)인 지금은 충분하다.

  # 노드는 private 서브넷에 둔다(공인 IP 없음 → 외부에서 직접 접근 불가).
  # 대신 ECR 이미지 풀·컨트롤플레인 통신은 NAT 를 통해 아웃바운드로만 나간다.
  enable_nat_gateway = true

  # NAT 는 1개만. 2개(AZ당 1개)면 시간당 요금이 2배이고 실습에는 과하다.
  # 잃는 것: ①이 AZ 가 죽으면 다른 AZ 노드도 인터넷이 끊긴다 ②AZ 를 넘는 트래픽에 데이터 전송 요금이 붙는다.
  # 운영 기준은 AZ당 1개. 3단계에서 NAT 자체를 걷어내고 VPC 엔드포인트로 바꾼다.
  single_nat_gateway = true

  # AWS Load Balancer Controller 가 Service type=LoadBalancer 를 만들 때
  # 이 태그를 보고 로드밸런서를 어느 서브넷에 둘지 결정한다. 없으면 LB 생성이 실패한다.
  public_subnet_tags  = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = { "kubernetes.io/role/internal-elb" = 1 }
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "노드그룹과 내부 LB 가 쓸 서브넷"
  value       = module.vpc.private_subnets
}
