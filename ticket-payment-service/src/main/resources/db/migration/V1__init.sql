-- 결제 기록과 Outbox 의 최초 스키마.
-- 이전에는 하이버네이트 ddl-auto: update 가 만들던 것을 옮겨오면서
-- NULL 허용과 varchar(255) 기본값을 의도한 값으로 다시 잡았다.

CREATE TABLE payment_record (
    order_id      varchar(13)  PRIMARY KEY,   -- TSID. 13자 고정.
    user_id       varchar(64)  NOT NULL,
    ticket_id     varchar(64)  NOT NULL,
    status        varchar(16)  NOT NULL,
    error_message varchar(255),               -- 실패했을 때만 채워지므로 NULL 허용
    created_at    timestamptz  NOT NULL,      -- PG 청구를 '시도한' 시각. 확정 시에도 갱신하지 않는다.
    CONSTRAINT payment_record_status_check
        CHECK (status IN ('SUCCESS', 'PENDING', 'FAILURE', 'CANCELLED'))
);

-- 스윕 배치(PaymentSweeper)의 조회 조건을 그대로 받친다.
--   WHERE status = 'PENDING' AND created_at < :threshold ORDER BY created_at
-- 확정된 결제가 대부분이고 PENDING 은 극소수이므로 부분 인덱스가 맞다.
CREATE INDEX idx_payment_pending
    ON payment_record (created_at)
    WHERE status = 'PENDING';

-- 행의 존재 자체가 '아직 발행되지 않음'을 뜻한다.
-- 발행에 성공하면 릴레이가 행을 삭제하므로 상태 컬럼이 없다(#22).
CREATE TABLE outbox_event (
    id         varchar(36) PRIMARY KEY,   -- UUID
    topic      varchar(64) NOT NULL,
    msg_key    varchar(64) NOT NULL,      -- 카프카 파티션 키. NULL 이면 라운드로빈이 되어 순서가 깨진다.
    payload    text        NOT NULL,
    created_at timestamptz NOT NULL
);

-- 릴레이가 created_at 순으로 집어간다.
--   SELECT * FROM outbox_event ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED
CREATE INDEX idx_outbox_created_at ON outbox_event (created_at);
