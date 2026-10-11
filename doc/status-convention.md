# 상태값 규칙

- 기준: `feature/event-interaction` (27b3997), `doc/kafka-convention.md`, `doc/TODO.txt` 4-2
- 상태: **제안안 (코드 미반영)**
- 다이어그램: `doc/status-diagram/` (order / saga / payment / shipment `.puml`)
- 원칙
  - 상태 enum은 그 상태를 가진 도메인 안에만 둡니다. 다른 도메인의 enum을 import하지 않습니다 (MSA 분리 대비).
  - 상태는 정해진 전이만 허용합니다. enum에 `canTransitionTo()`를 두고, 엔티티의 상태 변경 메서드에서 검증합니다.
  - 상태 변경과 이벤트 발행은 한 곳(엔티티 메서드 또는 오케스트레이터)에서만 합니다.

---

## 1. Order 주문 상태 `order/status/OrderStatus`

```
ORDERED → INVENTORY_REQUESTED → INVENTORY_CONFIRMED → PAYMENT_REQUESTED → PAID → SHIPPED → DELIVERED
   └──────────────┴────────────────────┴──────────────────┴─────────┴──→ CANCELLED
```

| 상태 | 의미 | 바뀌는 시점 |
|---|---|---|
| `ORDERED` | 주문 생성 | `POST /orders` |
| `INVENTORY_REQUESTED` | 재고 확인 요청함 | `stock.check` 발행 |
| `INVENTORY_CONFIRMED` | 재고 차감 완료 | `stock.deducted` / `stock.bulk.deducted` 수신 |
| `PAYMENT_REQUESTED` | 결제 요청함 | `payment.request` 발행 |
| `PAID` | 결제 완료 | `payment.succeeded` 수신 |
| `SHIPPED` | 배송 생성 (출고 준비) | `shipment.created` 수신 |
| `DELIVERED` | 배송 완료 | `shipment.completed` 수신 |
| `CANCELLED` | 취소 (최종) | 재고 부족, 결제 실패, 배송 실패, 사용자 취소 |

- 사용자 취소는 `PAYMENT_REQUESTED`까지만 가능합니다 (`PAID` 시점에 배송 요청이 나감).
- `INVENTORY_CANCELLED`는 삭제하고 `CANCELLED`로 합칩니다. 취소 사유는 사가의 `failureReason`에 남깁니다.
- `PARTIALLY_CANCELLED`(부분 취소)는 MVP 범위 밖이라 삭제하거나 보류로 표시합니다.
- 주석 수정: `SHIPPED`는 "배송 요청"이 아니라 "배송 생성", `DELIVERED`는 "배송 진행중"이 아니라 "배송 완료"입니다.

## 2. Saga 사가 상태 `order/orderEntity/OrderSaga`

### 2-1. `SagaStatus`

```
STARTED → IN_PROGRESS → COMPLETED
              ├─→ FAILED                       (보상할 게 없을 때: 재고 부족)
              └─→ COMPENSATING → COMPENSATED   (보상할 게 있을 때: 결제 실패, 배송 실패, 사용자 취소)
```

- `COMPLETED`는 `shipment.created`를 받은 시점입니다. 이후 배송 완료(`DELIVERED`)는 주문 상태만 바뀝니다.
- 최종 상태: `COMPLETED`, `FAILED`, `COMPENSATED`. 최종 상태가 된 뒤에 도착한 이벤트는 무시합니다 (중복 수신 대비).

### 2-2. `SagaStep` (현재 진행 단계)

| 값 | 의미 |
|---|---|
| `STOCK` | 재고 확인과 차감 |
| `PAYMENT` | 결제 |
| `SHIPMENT` | 배송 |

- `COMPENSATING`은 `SagaStep`에서 삭제합니다. 보상 중인지는 `SagaStatus`로 알 수 있습니다. step을 덮어쓰면 어느 단계부터 보상할지 알 수 없게 됩니다.

### 2-3. 단계별 결과 (사가가 들고 있는 참여자 상태)

| 필드 | 값 | 바뀌는 시점 |
|---|---|---|
| `productStatus` | `PENDING` → `DEDUCTED` / `INSUFFICIENT` → (`DEDUCTED`에서) `RESTORED` | stock 이벤트 수신 |
| `paymentStatus` | `PENDING` → `COMPLETED` / `FAILED` → (`COMPLETED`에서) `CANCELLED` | payment 이벤트 수신 |
| `shipmentStatus` | `PENDING` → `CREATED` / `FAILED` → (`CREATED`에서) `CANCELLED` | shipment 이벤트 수신 |

- 보상할 대상은 이 필드로 판단합니다. 예: `paymentStatus == COMPLETED`일 때만 `payment.cancel`을 보냅니다.
- `order/status/PaymentStatus`와 `payment/paymentDomain/PaymentStatus`는 값이 같지만 일부러 따로 둡니다. 앞의 것은 Order가 보는 결제 결과, 뒤의 것은 Payment가 직접 관리하는 상태입니다.

## 3. Payment 결제 상태 `payment/paymentDomain/PaymentStatus`

```
PENDING → COMPLETED → CANCELLED
   └────→ FAILED
```

| 전이 | 발행 이벤트 |
|---|---|
| `PENDING` → `COMPLETED` | `payment.succeeded` |
| `PENDING` → `FAILED` | `payment.failed` |
| `COMPLETED` → `CANCELLED` | `payment.cancelled` |

## 4. Shipment 배송 상태 `shipment/domain/ExternalShippingStatus`

```
READY → READY_FOR_PICKUP → SHIPPING → DELIVERED
  └──────────┴──→ CANCELLED            └─→ FAILED
```

| 전이 | 발행 이벤트 |
|---|---|
| 생성 → `READY` | `shipment.created` |
| `READY_FOR_PICKUP`, `SHIPPING` | `shipment.changed` (Order는 무시해도 됨) |
| → `DELIVERED` | `shipment.completed` |
| → `FAILED` | `shipment.failed` |
| → `CANCELLED` | `shipment.cancelled` (신규) |

- 이미 `canTransitionTo()`와 최종 상태 검사가 있습니다. Order와 Payment도 이 방식을 따릅니다.

## 5. Product

- 상품 엔티티에는 재고 처리 상태가 없습니다. 재고 수량만 있습니다.
- 재고 처리 결과는 이벤트(`stock.*`)로만 알리고, 사가의 `productStatus`에 기록합니다.

## 6. 현재 코드와의 차이 (변경할 것)

| 현재 | 변경 |
|---|---|
| `OrderStatus`에 전이 검증 없음 | `canTransitionTo()` 추가, `Order.updateStatus()`에서 검증 |
| `INVENTORY_CANCELLED`, `PARTIALLY_CANCELLED` | `CANCELLED`로 합침 / 삭제 |
| `SHIPPED`, `DELIVERED` 주석이 이름과 다름 | 1장의 의미로 수정 |
| `SagaStep.COMPENSATING`이 단계를 덮어씀 | 삭제 |
| `failStockDeduction()`이 step을 `COMPENSATING`으로 바꿈 (재고 부족은 보상할 게 없음) | `sagaStatus = FAILED` |
| `requestStockQtyChecked()`, `completeStockDeduction()`, `completePayment()` 등이 `sagaStatus`를 안 바꿈 | 2-1 흐름에 맞춰 `IN_PROGRESS` 등으로 갱신 |
| `ExternalShippingStatus.CANCELLED`가 `publishShipCompleted()`를 호출 | `shipment.cancelled` 발행 |
| `ShipmentStatusResponse`가 `payment.paymentDomain.PaymentStatus.PENDING`을 import | 삭제 (도메인 간 의존) |
| proto enum `OrderStatus`, `SagaStep`, `SagaType`, `SagaResult`, `OrderCancellationType`이 Java enum과 이름과 값이 다름 | 기준은 Java enum. proto enum과 이를 쓰는 미사용 메시지(`OrderStatusChangedEvent`, `SagaStartedEvent`, `SagaStepCompletedEvent`)는 삭제. 상태를 이벤트에 실어야 하면 enum 이름 문자열로 보냄 |
