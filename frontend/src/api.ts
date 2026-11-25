import type {
  ApiResult,
  DataQualityResponse,
  HealthResponse,
  MetricsResponse,
  OpsOverview,
  OrderRecord,
  QuarantineResponse,
  SilverJobState,
} from "./types";

interface Envelope<T> {
  success: boolean;
  message: string;
  data: T;
}

export async function apiRequest<T = unknown>(
  path: string,
  options: {
    method?: string;
    body?: unknown;
    headers?: Record<string, string>;
  } = {}
): Promise<ApiResult<T>> {
  const reqHeaders: Record<string, string> = {
    Accept: "application/json",
    ...(options.headers ?? {}),
  };

  let bodyStr: string | undefined;
  if (options.body !== undefined) {
    bodyStr = JSON.stringify(options.body);
    reqHeaders["Content-Type"] = "application/json";
  }

  const t0 = performance.now();
  try {
    const resp = await fetch(path, {
      method: options.method ?? "GET",
      headers: reqHeaders,
      body: bodyStr,
    });
    const elapsedMs = Math.round((performance.now() - t0) * 100) / 100;
    const text = await resp.text();
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = { detail: text };
    }
    return {
      status: resp.status,
      ok: resp.ok,
      data: parsed as T,
      requestId: resp.headers.get("x-request-id"),
      processTimeMs: resp.headers.get("x-process-time-ms"),
      elapsedMs,
    };
  } catch (err) {
    return {
      status: 0,
      ok: false,
      data: { detail: String(err), error_code: "CONNECTION_ERROR" } as T,
      requestId: null,
      processTimeMs: null,
      elapsedMs: Math.round((performance.now() - t0) * 100) / 100,
    };
  }
}

export async function fetchHealth(): Promise<ApiResult<HealthResponse>> {
  return apiRequest<HealthResponse>("/health");
}

export async function fetchMetrics(): Promise<ApiResult<MetricsResponse>> {
  return apiRequest<MetricsResponse>("/metrics");
}

export async function fetchOverview(): Promise<ApiResult<Envelope<OpsOverview>>> {
  return apiRequest<Envelope<OpsOverview>>("/api/v1/ops/overview");
}

export async function fetchOrders(params: {
  user_id?: string;
  product_id?: number;
  limit?: number;
  offset?: number;
}): Promise<ApiResult<Envelope<OrderRecord[]>>> {
  const qs = new URLSearchParams();
  if (params.user_id?.trim()) qs.set("user_id", params.user_id.trim());
  if (params.product_id && params.product_id > 0) qs.set("product_id", String(params.product_id));
  qs.set("limit", String(params.limit ?? 50));
  qs.set("offset", String(params.offset ?? 0));
  return apiRequest<Envelope<OrderRecord[]>>(`/api/v1/orders?${qs.toString()}`);
}

export async function fetchQuarantine(selectedKey?: string): Promise<ApiResult<Envelope<QuarantineResponse>>> {
  const qs = new URLSearchParams({ limit: "25" });
  if (selectedKey) qs.set("key", selectedKey);
  return apiRequest<Envelope<QuarantineResponse>>(`/api/v1/ops/quarantine?${qs.toString()}`);
}

export async function fetchDataQuality(): Promise<ApiResult<Envelope<DataQualityResponse>>> {
  return apiRequest<Envelope<DataQualityResponse>>("/api/v1/ops/data-quality");
}

export async function fetchSilverJob(): Promise<ApiResult<Envelope<SilverJobState>>> {
  return apiRequest<Envelope<SilverJobState>>("/api/v1/ops/jobs/silver-stream");
}

export function generateKey(prefix = "ui"): string {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
}

export function formatLocalDateTime(iso: string | null | undefined): string {
  if (!iso) return "-";
  const trimmed = iso.trim();
  const hasTz = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(trimmed);
  const d = new Date(hasTz ? trimmed : `${trimmed}Z`);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("vi-VN", { hour12: false });
}
