import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Activity, Cpu, Database, HardDrive } from "lucide-react";
import type { MetricsResponse, OpsOverview } from "../types";

interface Props {
  overview: OpsOverview | null;
  metrics: MetricsResponse | null;
}

export function OverviewTab({ overview, metrics }: Props) {
  const counters = metrics?.counters ?? {};
  const pool = metrics?.db_pool;

  const statusData = [
    { name: "200 OK", value: counters.http_status_200_total ?? 0 },
    { name: "201 Created", value: counters.http_status_201_total ?? 0 },
    { name: "400 OutOfStock", value: counters.http_status_400_total ?? 0 },
    { name: "409 Conflict", value: counters.http_status_409_total ?? 0 },
    { name: "500 Error", value: counters.http_status_500_total ?? 0 },
  ];

  const gateData = [
    { name: "Cache Hits", value: counters.product_cache_hits_total ?? 0 },
    { name: "Cache Misses", value: counters.product_cache_misses_total ?? 0 },
    { name: "Lua Rejects", value: counters.redis_gate_rejected_out_of_stock_total ?? 0 },
    { name: "DB Committed", value: counters.orders_committed_total ?? 0 },
  ];

  const outboxPending =
    (overview?.outbox.counts.PENDING ?? 0) + (overview?.outbox.counts.FAILED ?? 0);

  return (
    <div className="space-y-5">
      {/* KPI Row */}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
        <KpiCard
          label="Confirmed Orders"
          value={(overview?.oltp.total_orders ?? 0).toLocaleString()}
          sub="OLTP PostgreSQL"
        />
        <KpiCard
          label="Gross Revenue"
          value={`$${(overview?.oltp.total_revenue ?? 0).toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}`}
          sub="Committed total"
        />
        <KpiCard
          label="Avg OLTP Latency"
          value={`${(overview?.oltp.avg_latency_ms ?? 0).toFixed(1)} ms`}
          sub="Server processing"
        />
        <KpiCard
          label="Product #1 RAM Stock"
          value={
            metrics?.product_1_redis_stock !== null &&
            metrics?.product_1_redis_stock !== undefined
              ? String(metrics.product_1_redis_stock)
              : "N/A"
          }
          sub="Redis atomic counter"
        />
        <KpiCard
          label="Outbox Queue"
          value={String(outboxPending)}
          sub={outboxPending === 0 ? "All events published" : "Needs replay"}
          tone={outboxPending === 0 ? "ok" : "warn"}
        />
      </div>

      {/* Multi-worker Counters Strip */}
      <div className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
            <Activity size={15} strokeWidth={1.8} className="text-blue-600 dark:text-blue-400" />
            <span>Multi-Worker Telemetry (/metrics)</span>
          </div>
          {pool && (
            <div className="flex items-center gap-4 text-xs text-slate-500 dark:text-zinc-400 num font-mono">
              <span className="inline-flex items-center gap-1">
                <Database size={13} /> pool={pool.pool_size}
              </span>
              <span>in={pool.checked_in}</span>
              <span>out={pool.checked_out}</span>
              <span>overflow={pool.overflow}</span>
            </div>
          )}
        </div>

        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          <MiniStat label="HTTP Requests" value={counters.http_requests_total ?? 0} />
          <MiniStat label="Cache Hits" value={counters.product_cache_hits_total ?? 0} />
          <MiniStat label="Lua Rejects" value={counters.redis_gate_rejected_out_of_stock_total ?? 0} />
          <MiniStat label="DB Committed" value={counters.orders_committed_total ?? 0} />
          <MiniStat label="Kafka Published" value={counters.kafka_events_published_total ?? 0} />
          <MiniStat label="Kafka Failures" value={counters.kafka_publish_failures_total ?? 0} />
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <div className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
            <Cpu size={15} strokeWidth={1.8} className="text-slate-500" />
            <span>HTTP Status Distribution (4 Workers)</span>
          </div>
          <div className="h-60 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={statusData} margin={{ top: 8, right: 12, left: -12, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#94a3b8" strokeOpacity={0.2} />
                <XAxis dataKey="name" tick={{ fontSize: 11, fill: "#64748b" }} />
                <YAxis tick={{ fontSize: 11, fill: "#64748b" }} />
                <Tooltip
                  contentStyle={{
                    fontSize: "12px",
                    borderRadius: "6px",
                    border: "1px solid #cbd5e1",
                  }}
                />
                <Bar dataKey="value" fill="#2563eb" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
            <HardDrive size={15} strokeWidth={1.8} className="text-slate-500" />
            <span>Redis Gate vs PostgreSQL Acquisitions</span>
          </div>
          <div className="h-60 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={gateData} margin={{ top: 8, right: 12, left: -12, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#94a3b8" strokeOpacity={0.2} />
                <XAxis dataKey="name" tick={{ fontSize: 11, fill: "#64748b" }} />
                <YAxis tick={{ fontSize: 11, fill: "#64748b" }} />
                <Tooltip
                  contentStyle={{
                    fontSize: "12px",
                    borderRadius: "6px",
                    border: "1px solid #cbd5e1",
                  }}
                />
                <Bar dataKey="value" fill="#0f766e" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}

function KpiCard({
  label,
  value,
  sub,
  tone = "default",
}: {
  label: string;
  value: string;
  sub: string;
  tone?: "default" | "ok" | "warn";
}) {
  const accent =
    tone === "ok"
      ? "text-emerald-600 dark:text-emerald-400"
      : tone === "warn"
      ? "text-amber-600 dark:text-amber-400"
      : "text-slate-900 dark:text-zinc-50";

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
      <div className="text-xs font-medium text-slate-500 dark:text-zinc-400">{label}</div>
      <div className={`mt-1 text-xl font-semibold tracking-tight num font-mono ${accent}`}>{value}</div>
      <div className="mt-1 text-[11px] text-slate-400 dark:text-zinc-500">{sub}</div>
    </div>
  );
}

function MiniStat({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border border-slate-100 bg-slate-50/70 px-3 py-2 dark:border-zinc-800 dark:bg-zinc-950/60">
      <div className="text-[11px] text-slate-500 dark:text-zinc-400">{label}</div>
      <div className="mt-0.5 text-sm font-semibold text-slate-800 dark:text-zinc-100 num font-mono">
        {value.toLocaleString()}
      </div>
    </div>
  );
}
