# Kafka 토픽 / 키 / 메시지 규칙

- 기준: `feature/event-interaction` (27b3997), `src/designEDA.puml`, `doc/TODO.txt` 4-1
- 흐름: 주문 생성 → 재고 차감 → 결제 → 배송 (오케스트레이터 = Order)
- 상태: **제안안 (코드 미반영)**

---

## 1. 기본 규칙

| 항목 | 규칙 |
|---|---|
| 토픽 | 요청용 `{도메인}-commands`, 결과용 `{도메인}-events` |
| 키 | `orderUUID` (같은 주문의 메시지는 같은 파티션으로 가서 순서가 보장됨) |
| 이벤트 타입 | 헤더 `eventType` (키에 넣지 않음) |
| 멱등 키 | 헤더 `eventId` (UUID). 컨슈머는 처리한 eventId를 기록해서 중복 수신을 무시 |
| 값 | Protobuf `byte[]`. eventType 하나에 proto 메시지 하나 |
| 이름 | 요청은 `도메인.동사` (`stock.deduct`), 결과는 `도메인.과거분사` (`stock.deducted`). 예외: 기존 이름을 유지하는 `stock.check.response` |
| 컨슈머 그룹 | `{서비스}-service`. 다른 서비스의 그룹 ID는 쓰지 않음 |
| 상수 | 토픽 이름과 eventType 문자열은 `KafkaTopics`, `EventTypes` 상수 클래스에서만 정의 |

## 2. 토픽

| 토픽 | 종류 | 발행 | 구독 그룹 |
|---|---|---|---|
| `order-events` | 결과 | Order | `order-service` (오케스트레이터) |
| `product-commands` | 요청 | Order | `product-service` |
| `product-events` | 결과 | Product | `order-service` |
| `payment-commands` | 요청 | Order | `payment-service` |
| `payment-events` | 결과 | Payment | `order-service` |
| `shipment-commands` | 요청 | Order | `shipment-service` |
| `shipment-events` | 결과 | Shipment | `order-service` |

자기가 발행한 결과 토픽을 자기가 구독하는 경우는 `order-events` 하나뿐입니다.

## 3. 메시지 목록

| 단계 | eventType | 토픽 | 방향 | proto 메시지 |
|---|---|---|---|---|
| 주문 | `order.created` | order-events | Order → 오케스트레이터 | `OrderCreatedEvent` |
| 주문 | `order.cancelled` | order-events | Order → 오케스트레이터 | `OrderCancelledEvent` |
| 재고 확인 | `stock.check` | product-commands | Order → Product | `SagaStockCheckEvent` |
| 재고 확인 | `stock.check.response` | product-events | Product → Order | `StockCheckResponseEvent` (신규) |
| 단순 차감 | `stock.deduct` | product-commands | Order → Product | `SagaStockDeductEvent` (상품 1개) |
| 단순 차감 | `stock.deducted` | product-events | Product → Order | `StockDeductEvent` |
| 단순 차감 | `stock.lacked` | product-events | Product → Order | `StockDeductLackedEvent` |
| 배치 차감 | `stock.bulk.deduct` | product-commands | Order → Product | `SagaStockDeductEvent` (상품 N개) |
| 배치 차감 | `stock.bulk.deducted` | product-events | Product → Order | `StockDeductEvent` |
| 배치 차감 | `stock.bulk.lacked` | product-events | Product → Order | `StockDeductLackedEvent` |
| 재고 (보상) | `stock.restore` | product-commands | Order → Product | `SagaStockRestoreEvent` |
| 재고 (보상) | `stock.restored` | product-events | Product → Order | `StockRestoredEvent` |
| 결제 | `payment.request` | payment-commands | Order → Payment | `SagaPaymentRequestEvent` |
| 결제 | `payment.succeeded` | payment-events | Payment → Order | `PaymentSuccessEvent` |
| 결제 | `payment.failed` | payment-events | Payment → Order | `PaymentFailedEvent` |
| 결제 (보상) | `payment.cancel` | payment-commands | Order → Payment | `SagaPaymentCancelRequestEvent` |
| 결제 (보상) | `payment.cancelled` | payment-events | Payment → Order | `PaymentCancelledEvent` |
| 배송 | `shipment.request` | shipment-commands | Order → Shipment | `SagaShipmentRequestEvent` |
| 배송 | `shipment.created` | shipment-events | Shipment → Order | `ShipmentCreatedEvent` |
| 배송 | `shipment.failed` | shipment-events | Shipment → Order | `ShipmentFailedEvent` |
| 배송 | `shipment.completed` | shipment-events | Shipment → Order | `ShipmentCompletedEvent` |
| 배송 | `shipment.changed` | shipment-events | Shipment → (Order 무시) | `ShipmentStatusChangedEvent` |
| 배송 | `shipment.cancelled` | shipment-events | Shipment → Order | 신규 (상태 문서 4장) |

- 재고 이벤트는 `ActiveStockEventPublisher`의 6개 이벤트를 그대로 씁니다. 단순 차감과 배치 차감을 둘 다 둡니다.
  - 차감 방식: 주문 상품이 1개면 `stock.deduct`, 2개 이상이면 `stock.bulk.deduct`
  - 배치 차감은 전부 성공하거나 전부 실패합니다. 하나라도 부족하면 전체를 롤백하고 `stock.bulk.lacked`를 발행합니다.
  - `stock.check`는 미리 확인만 하고 재고를 예약하지 않습니다. 실제 보장은 차감할 때 비관적 락 안에서 다시 검사해서 얻습니다.
  - `stock.check.response`에는 `orderUUID`와 확인 결과가 들어가야 합니다. 그래서 전용 메시지를 새로 만듭니다.
  - Product는 주문별 차감 이력(`StockDeductHistory`)으로 중복 차감을 무시하고, `stock.restore`는 요청 수량이 아니라 미복구 이력 수량으로 복구합니다.
- `order.cancelled`를 받은 오케스트레이터는 보상 요청만 보내고, `order.cancelled`를 다시 발행하지 않습니다 (취소 루프 방지).

## 4. 보상 규칙

| 실패 시점 | 보낼 보상 요청 |
|---|---|
| `stock.check.response` (부족) | 없음 (주문만 CANCELLED) |
| `stock.lacked` / `stock.bulk.lacked` | 없음 (주문만 CANCELLED, 배치는 Product 안에서 이미 롤백됨) |
| `payment.failed` | `stock.restore` |
| `shipment.failed` | `payment.cancel` → `stock.restore` |
| 사용자 취소 (`order.cancelled`) | 진행된 단계를 역순으로 보상 |

## 5. 현재 코드와의 차이 (변경할 것)

| 현재 | 변경 |
|---|---|
| 키 = eventType 문자열 | 키 = orderUUID, eventType은 헤더로 |
| 요청과 결과가 같은 토픽 (`product-events` 등) | `*-commands`와 `*-events`로 분리 |
| OrderSagaEventConsumer가 `product-service`, `payment-service`, `shipment-service` 그룹 사용 | 모두 `order-service` |
| OrderEventConsumer와 OrderSagaEventConsumer가 `order-events`를 중복 구독 | 오케스트레이터 하나만 구독 |
| 배치 차감 실패도 `stock.lacked`로 발행 (단순 차감 실패와 구분 안 됨) | `stock.bulk.lacked` |
| 배치 차감 요청 키가 따로 없음 (`stock.deduct` 하나로 `deductStocks` 호출) | `stock.deduct` → `deductStock`, `stock.bulk.deduct` → `deductStocks` |
| `stock.check.response`가 빈 `StockDeductEvent` (orderUUID와 결과 없음) | `StockCheckResponseEvent { order_uuid, available, repeated LackedItems }` |
| `stock.restore`를 `order-event`(오타)로 발행 | `product-commands` |
| `saga.payment.request` (Payment는 `saga-events`의 `order.created`를 구독) | `payment.request` on `payment-commands` |
| `payment.success` | `payment.succeeded` |
| `payment.cancellation.requested`, 빈 `byte[]` | `payment.cancel` + `SagaPaymentCancelRequestEvent` |
| `shipment.requested` | `shipment.request` on `shipment-commands` |
| 보내는 쪽과 받는 쪽의 proto 타입이 다름 (`SagaStockDeductEvent` → `StockDeductEvent`, `SagaPaymentRequestEvent` → `PaymentRequestedEvent`, `SagaShipmentRequestEvent` → `OrderLifecycleEvent`) | 3장의 proto 메시지로 통일. 나머지 proto는 정리 (TODO 6) |
