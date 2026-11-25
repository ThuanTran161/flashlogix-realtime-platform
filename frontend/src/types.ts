export interface HealthResponse {
  status: string;
  app: string;
  environment: string;
  concurrency_mode: string;
  services: {
    redis: string;
    database: string;
    kafka: string;
    minio: string;
  };
}

export interface MetricsResponse {
  app: string;
  concurrency_mode: string;
  uptime_seconds: number;
  counters: Record<string, number>;
  db_pool: {
    pool_size: number;
    checked_in: number;
    checked_out: number;
    overflow: number;
  };
  oltp_orders_total: number;
  outbox_pending_or_failed: number;
  product_1_redis_stock: number | null;
}

export interface InventoryItem {
  id: number;
  name: string;
  price: number;
  db_stock: number;
  redis_stock: number | null;
  in_sync: boolean;
  version: number;
}

export interface OutboxEventItem {
  id: number;
  aggregate_id: string;
  status: string;
  attempts: number;
  last_error: string | null;
  created_at: string | null;
  published_at: string | null;
}

export interface OpsOverview {
  oltp: {
    total_orders: number;
    total_revenue: number;
    avg_latency_ms: number;
  };
  inventory: InventoryItem[];
  outbox: {
    counts: Record<string, number>;
    recent: OutboxEventItem[];
  };
  gold: {
    fact_orders: number;
    total_revenue: number;
    fraud_flagged: number;
    tiers: Array<{ membership_tier: string; user_count: number }>;
  } | null;
  lakehouse: {
    bronze: number;
    silver: number;
    gold: number;
    quarantine: number;
  } | null;
}

export interface OrderRecord {
  id: number;
  user_id: string;
  product_id: number;
  quantity: number;
  total_amount: number;
  status: string;
  latency_ms: number;
  created_at: string;
}

export interface QuarantineResponse {
  objects: Array<{
    key: string;
    size: number;
    last_modified: string | null;
  }>;
  preview: unknown;
}

export interface SilverJobState {
  status: "IDLE" | "RUNNING" | "COMPLETED" | "FAILED";
  started_at: string | null;
  finished_at: string | null;
  duration_sec: number | null;
  log_tail: string[];
}

export interface DataQualityResponse {
  available: boolean;
  report: Record<string, unknown> | null;
}

export interface ApiResult<T = unknown> {
  status: number;
  ok: boolean;
  data: T;
  requestId: string | null;
  processTimeMs: string | null;
  elapsedMs: number;
}
