import random
import uuid
from locust import HttpUser, between, task


class FlashSaleShopper(HttpUser):
    """Simulates high-velocity shopper behavior during a Flash Sale campaign.

    Tests zero-overselling compliance under 1,000+ virtual users firing requests
    at sub-second intervals.
    """

    # Realistic click pause: 50ms - 200ms
    wait_time = between(0.05, 0.20)

    @task(10)
    def purchase_flash_item(self):
        """Simulates competing shoppers rushing to purchase the limited product #1."""
        simulated_user_id = f"user_{random.randint(1, 10000)}"
        idempotency_token = f"tok_{uuid.uuid4().hex[:16]}"

        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_token,
        }
        payload = {
            "user_id": simulated_user_id,
            "product_id": 1,
            "quantity": 1,
        }

        with self.client.post("/orders", json=payload, headers=headers, catch_response=True) as response:
            if response.status_code == 201:
                response.success()
            elif response.status_code == 400:
                # 400 Bad Request is the expected, correct behavior once stock hits 0
                resp_json = response.json()
                if "error_code" in resp_json and resp_json["error_code"] == "INSUFFICIENT_STOCK":
                    response.success()
                else:
                    response.failure(f"Unexpected 400 payload: {response.text}")
            elif response.status_code == 409:
                # Handled conflict / duplicate
                response.success()
            else:
                response.failure(f"Critical error (status {response.status_code}): {response.text}")

    @task(1)
    def view_product_stock(self):
        """Simulates users constantly refreshing the product page to check stock."""
        with self.client.get("/api/v1/products/1", catch_response=True) as response:
            if response.status_code in (200, 404):
                response.success()
            else:
                response.failure(f"Failed to view product: {response.status_code}")
