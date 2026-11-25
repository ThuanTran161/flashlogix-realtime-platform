import sys
import uuid
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.core.config import settings
from app.core.database import SessionLocal, engine, ensure_oltp_schema_and_constraints
from app.main import app
from app.schemas.order import OrderCreate
from app.schemas.product import ProductCreate
from app.services.inventory_service import inventory_service, redis_client
from app.services.kafka_producer import event_publisher

_created_test_product_ids = []


@pytest.fixture(scope="module", autouse=True)
def setup_test_db():
    ensure_oltp_schema_and_constraints()
    # Clean up any leftover negative-stock row if an earlier un-constrained run inserted one
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM products WHERE stock < 0"))
    yield
    if _created_test_product_ids:
        with engine.begin() as conn:
            for pid in _created_test_product_ids:
                conn.execute(text("DELETE FROM orders WHERE product_id = :pid"), {"pid": pid})
                conn.execute(text("DELETE FROM products WHERE id = :pid"), {"pid": pid})
                redis_client.delete(f"stock:product:{pid}", f"product:meta:{pid}")


client = TestClient(app)


def test_settings_load():
    """Verifies environment settings are loaded and typed correctly."""
    assert settings.APP_NAME == "FlashLogix"
    assert settings.PORT == 8000
    assert settings.CONCURRENCY_MODE == "redis_async_lazy"
    assert settings.KAFKA_TOPIC_ORDERS_DLQ == "flashsale.orders.dlq"
    assert "postgresql://" in settings.get_database_url()


def test_health_and_observability_endpoints():
    """Verifies /health, X-Request-ID/X-Process-Time-Ms headers, and /metrics (JSON & Prometheus)."""
    response = client.get("/health", headers={"X-Request-ID": "trace_abc_123"})
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "trace_abc_123"
    assert float(response.headers["X-Process-Time-Ms"]) >= 0.0
    data = response.json()
    assert data["status"] == "UP"
    assert data["app"] == "FlashLogix"
    assert "services" in data

    m_json = client.get("/metrics")
    assert m_json.status_code == 200
    m_data = m_json.json()
    assert "counters" in m_data
    assert "db_pool" in m_data
    assert m_data["counters"]["http_requests_total"] >= 1

    m_prom = client.get("/metrics?format=prometheus")
    assert m_prom.status_code == 200
    assert "flashlogix_uptime_seconds" in m_prom.text
    assert "flashlogix_oltp_orders_total" in m_prom.text


def test_pydantic_order_validation():
    """Verifies Pydantic v2 validation constraints on Order schema."""
    valid_payload = OrderCreate(user_id="user_123", product_id=1, quantity=2)
    assert valid_payload.user_id == "user_123"
    assert valid_payload.quantity == 2

    with pytest.raises(Exception):
        OrderCreate(user_id="user_123", product_id=1, quantity=-1)


def test_pydantic_product_validation():
    """Verifies product validation schema."""
    valid_product = ProductCreate(name="iPhone 16 Pro Max", price=1299.99, stock=100)
    assert valid_product.stock == 100
    assert valid_product.price == 1299.99

    with pytest.raises(Exception):
        ProductCreate(name="Bad Price", price=-10.0, stock=10)


def test_redis_atomic_lua_decrement_and_rollback():
    """Tests Redis Lua atomic decrement, out-of-stock guard, and compensatory rollback."""
    test_pid = 99991
    redis_client.delete(f"stock:product:{test_pid}")

    # Cache miss returns (False, -2)
    ok, code = inventory_service.deduct_stock_atomic(test_pid, 1)
    assert ok is False
    assert code == -2

    # Initialize stock = 2
    inventory_service.initialize_stock(test_pid, 2)
    assert inventory_service.get_stock(test_pid) == 2

    # Deduct 1 -> remaining 1
    ok, rem = inventory_service.deduct_stock_atomic(test_pid, 1)
    assert ok is True
    assert rem == 1

    # Deduct 2 when only 1 left -> rejected (-1), stock remains 1
    ok, code = inventory_service.deduct_stock_atomic(test_pid, 2)
    assert ok is False
    assert code == -1
    assert inventory_service.get_stock(test_pid) == 1

    # Compensatory increment restores stock
    restored = inventory_service.compensatory_increment(test_pid, 1)
    assert restored == 2
    redis_client.delete(f"stock:product:{test_pid}")


def test_order_idempotency_query_and_lazy_db_skip(monkeypatch):
    """Tests Create Product -> List Products -> Order -> Get Order -> Idempotency & Lazy DB Skip."""
    published_events = []
    monkeypatch.setattr(
        event_publisher,
        "publish_order_event",
        lambda payload, topic=None: published_events.append(payload) or True,
    )

    prod_resp = client.post(
        "/api/v1/products",
        json={"name": f"Test Flash Item {uuid.uuid4().hex[:6]}", "price": 99.5, "stock": 1},
    )
    assert prod_resp.status_code == 201
    product_id = prod_resp.json()["data"]["id"]
    _created_test_product_ids.append(product_id)

    # Verify list products endpoint includes created product
    list_resp = client.get("/api/v1/products")
    assert list_resp.status_code == 200
    assert any(item["id"] == product_id for item in list_resp.json()["data"])

    idem_key = f"idem_{uuid.uuid4().hex}"
    headers = {"Idempotency-Key": idem_key}

    # First order succeeds
    r1 = client.post(
        "/api/v1/orders",
        json={"user_id": "test_buyer_1", "product_id": product_id, "quantity": 1},
        headers=headers,
    )
    assert r1.status_code == 201
    order_id_1 = r1.json()["data"]["id"]
    assert r1.json()["data"]["total_amount"] == 99.5
    assert len(published_events) == 1

    # Verify GET /api/v1/orders/{order_id}
    get_order_resp = client.get(f"/api/v1/orders/{order_id_1}")
    assert get_order_resp.status_code == 200
    assert get_order_resp.json()["data"]["id"] == order_id_1

    # Patch SessionLocal on the actual modules to prove retries & rejections NEVER touch PostgreSQL
    order_service_mod = sys.modules["app.services.order_service"]
    products_api_mod = sys.modules["app.api.v1.products"]

    def fail_if_db_opened():
        raise AssertionError("PostgreSQL SessionLocal should NOT be acquired on cache hit/rejection!")

    monkeypatch.setattr(order_service_mod, "SessionLocal", fail_if_db_opened)
    monkeypatch.setattr(products_api_mod, "SessionLocal", fail_if_db_opened)

    # Retry with same Idempotency-Key returns same order from Redis without opening DB or double-emitting Kafka
    r2 = client.post(
        "/api/v1/orders",
        json={"user_id": "test_buyer_1", "product_id": product_id, "quantity": 1},
        headers=headers,
    )
    assert r2.status_code == 201
    assert r2.json()["data"]["id"] == order_id_1
    assert len(published_events) == 1

    # Second distinct buyer fails with INSUFFICIENT_STOCK in Redis without touching DB
    r3 = client.post(
        "/api/v1/orders",
        json={"user_id": "test_buyer_2", "product_id": product_id, "quantity": 1},
        headers={"Idempotency-Key": f"idem_{uuid.uuid4().hex}"},
    )
    assert r3.status_code == 400
    assert r3.json()["error_code"] == "INSUFFICIENT_STOCK"
    assert inventory_service.get_stock(product_id) == 0

    # Product read hits Redis Read-Through Cache with updated RAM stock=0 and zero DB queries
    p_get = client.get(f"/api/v1/products/{product_id}")
    assert p_get.status_code == 200
    assert p_get.json()["data"]["stock"] == 0


def test_compensatory_rollback_on_db_failure(monkeypatch):
    """Verifies Redis stock is automatically restored if PostgreSQL commit fails."""
    prod_resp = client.post(
        "/api/v1/products",
        json={"name": f"Rollback Item {uuid.uuid4().hex[:6]}", "price": 50.0, "stock": 5},
    )
    assert prod_resp.status_code == 201
    product_id = prod_resp.json()["data"]["id"]
    _created_test_product_ids.append(product_id)

    from sqlalchemy.orm import Session

    def failing_commit(self):
        raise RuntimeError("Simulated PostgreSQL outage")

    monkeypatch.setattr(Session, "commit", failing_commit)

    resp = client.post(
        "/api/v1/orders",
        json={"user_id": "rollback_user", "product_id": product_id, "quantity": 2},
        headers={"Idempotency-Key": f"idem_{uuid.uuid4().hex}"},
    )
    assert resp.status_code == 500
    assert resp.json()["error_code"] == "DATABASE_ERROR"

    # Crucial assertion: Redis stock must still be 5 after compensatory INCRBY
    assert inventory_service.get_stock(product_id) == 5


def test_db_check_constraint_blocks_negative_stock():
    """Defense-in-depth: PostgreSQL CheckConstraint rejects negative product stock."""
    from sqlalchemy.exc import IntegrityError
    from app.models.product import Product

    db = SessionLocal()
    try:
        bad_prod = Product(name="Negative Stock Test", price=10.0, stock=-1)
        db.add(bad_prod)
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()
    finally:
        db.close()
