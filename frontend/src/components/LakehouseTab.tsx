import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Database, GitBranch, Layers, Play } from "lucide-react";
import { apiRequest, fetchSilverJob } from "../api";
import type { OpsOverview, SilverJobState } from "../types";

interface Props {
  overview: OpsOverview | null;
  onRefresh: () => void;
}

export function LakehouseTab({ overview, onRefresh }: Props) {
  const [targetDate, setTargetDate] = useState("");
  const [goldRunning, setGoldRunning] = useState(false);
  const [goldMsg, setGoldMsg] = useState<string | null>(null);
  const [silverJob, setSilverJob] = useState<SilverJobState | null>(null);

  async function pollSilver() {
    const res = await fetchSilverJob();
    if (res.ok && res.data?.data) {
      setSilverJob(res.data.data);
    }
  }

  useEffect(() => {
    void pollSilver();
  }, []);

  useEffect(() => {
    if (silverJob?.status !== "RUNNING") return;
    const id = setInterval(() => {
      void pollSilver();
    }, 2500);
    return () => clearInterval(id);
  }, [silverJob?.status]);

  async function handleStartSilver() {
    const res = await apiRequest<{ data: SilverJobState }>("/api/v1/ops/jobs/silver-stream", {
      method: "POST",
    });
    if (res.ok && res.data?.data) {
      setSilverJob(res.data.data);
    }
  }

  async function handleRunGold() {
    setGoldRunning(true);
    setGoldMsg(null);
    const res = await apiRequest<{ data: Record<string, unknown> }>("/api/v1/ops/jobs/gold-etl", {
      method: "POST",
      body: { target_date: targetDate.trim() || null },
    });
    if (res.ok && res.data?.data) {
      const dq = (res.data.data.data_quality ?? {}) as Record<string, unknown>;
      setGoldMsg(
        `Done in ${dq.etl_duration_sec ?? "?"}s | Source=${dq.source_layer ?? "?"} | Processed=${
          res.data.data.processed_orders ?? "?"
        } | Parity=${dq.order_parity_pct ?? "?"}% | Delta=$${dq.revenue_delta ?? "?"}`
      );
      onRefresh();
    } else {
      setGoldMsg(`Gold ETL error (${res.status}): ${JSON.stringify(res.data)}`);
    }
    setGoldRunning(false);
  }

  const lakeChart = overview?.lakehouse
    ? [
        { layer: "BRONZE", files: overview.lakehouse.bronze },
        { layer: "SILVER", files: overview.lakehouse.silver },
        { layer: "GOLD", files: overview.lakehouse.gold },
        { layer: "QUARANTINE", files: overview.lakehouse.quarantine },
      ]
    : [];

  const tierChart = overview?.gold?.tiers ?? [];
  const pendingGoldOrders = overview?.gold
    ? Math.max(0, overview.oltp.total_orders - overview.gold.fact_orders)
    : 0;

  return (
    <div className="space-y-5">
      {/* ETL Triggers */}
      <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
        <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
          <GitBranch size={15} className="text-slate-500" />
          <span>Pipeline Controls (PySpark Silver & Gold Star Schema)</span>
        </div>

        <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
          <button
            type="button"
            disabled={silverJob?.status === "RUNNING"}
            onClick={handleStartSilver}
            className="inline-flex items-center justify-center gap-1.5 rounded-md border border-slate-300 bg-white px-3 py-2 text-xs font-medium text-slate-800 hover:bg-slate-50 disabled:opacity-50 dark:border-zinc-700 dark:bg-zinc-950 dark:text-zinc-100"
          >
            <Play size={13} />
            {silverJob?.status === "RUNNING"
              ? "PySpark Silver Đang Chạy..."
              : "Chạy PySpark Silver (--available-now)"}
          </button>

          <input
            type="text"
            value={targetDate}
            onChange={(e) => setTargetDate(e.target.value)}
            placeholder="UTC date pruning (YYYY-MM-DD, trống = ALL)"
            className="rounded-md border border-slate-300 bg-white px-3 py-2 text-xs font-mono dark:border-zinc-700 dark:bg-zinc-950"
          />

          <button
            type="button"
            disabled={goldRunning}
            onClick={handleRunGold}
            className="inline-flex items-center justify-center gap-1.5 rounded-md bg-blue-600 px-3 py-2 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
          >
            <Layers size={13} />
            {goldRunning ? "Đang chạy Gold ETL..." : "Chạy Gold Star Schema ETL"}
          </button>
        </div>

        {goldMsg && (
          <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs font-mono text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/50 dark:text-emerald-300">
            {goldMsg}
          </div>
        )}

        {silverJob && silverJob.status !== "IDLE" && (
          <div className="mt-3 space-y-1.5 rounded-md border border-slate-200 bg-slate-50 p-3 dark:border-zinc-800 dark:bg-zinc-950">
            <div className="flex items-center justify-between text-xs font-mono">
              <span>
                Silver Job: <strong>{silverJob.status}</strong>{" "}
                <span className="text-[11px] font-normal text-slate-500">(Spark window theo giờ UTC)</span>
              </span>
              {silverJob.duration_sec !== null && <span>{silverJob.duration_sec}s</span>}
            </div>
            {silverJob.log_tail.length > 0 && (
              <pre className="max-h-32 overflow-auto text-[11px] text-slate-600 dark:text-zinc-400">
                {silverJob.log_tail.join("\n")}
              </pre>
            )}
          </div>
        )}
      </section>

      {/* Medallion + Gold Star Schema */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
            <Database size={15} className="text-slate-500" />
            <span>MinIO Medallion Objects</span>
          </div>
          <div className="h-56 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={lakeChart} margin={{ top: 8, right: 12, left: -12, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#94a3b8" strokeOpacity={0.2} />
                <XAxis dataKey="layer" tick={{ fontSize: 11, fill: "#64748b" }} />
                <YAxis tick={{ fontSize: 11, fill: "#64748b" }} />
                <Tooltip />
                <Bar dataKey="files" fill="#334155" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </section>

        <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <div className="mb-3 text-sm font-semibold text-slate-800 dark:text-zinc-100">
            Gold Star Schema & SCD Type 2 (`Dim_Users`)
          </div>

          {overview?.gold ? (
            <div className="space-y-4">
              <div className="grid grid-cols-3 gap-2">
                <div className="rounded border border-slate-100 bg-slate-50 p-2.5 dark:border-zinc-800 dark:bg-zinc-950">
                  <div className="text-[11px] text-slate-500">Fact Orders</div>
                  <div className="font-mono text-base font-semibold num">
                    {overview.gold.fact_orders.toLocaleString()}
                  </div>
                  <div
                    className={`mt-0.5 text-[10px] ${
                      pendingGoldOrders > 0
                        ? "text-amber-600 dark:text-amber-400"
                        : "text-emerald-600 dark:text-emerald-400"
                    }`}
                  >
                    {pendingGoldOrders > 0
                      ? `+${pendingGoldOrders} đơn mới chờ Gold ETL`
                      : `Khớp 100% OLTP (${overview.oltp.total_orders})`}
                  </div>
                </div>
                <div className="rounded border border-slate-100 bg-slate-50 p-2.5 dark:border-zinc-800 dark:bg-zinc-950">
                  <div className="text-[11px] text-slate-500">Gold Revenue</div>
                  <div className="font-mono text-base font-semibold num">
                    ${overview.gold.total_revenue.toLocaleString()}
                  </div>
                </div>
                <div className="rounded border border-slate-100 bg-slate-50 p-2.5 dark:border-zinc-800 dark:bg-zinc-950">
                  <div className="text-[11px] text-slate-500">Fraud Flagged</div>
                  <div className="font-mono text-base font-semibold text-amber-600 num">
                    {overview.gold.fraud_flagged.toLocaleString()}
                  </div>
                </div>
              </div>

              <div className="h-36 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={tierChart} margin={{ top: 4, right: 12, left: -12, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#94a3b8" strokeOpacity={0.2} />
                    <XAxis dataKey="membership_tier" tick={{ fontSize: 11, fill: "#64748b" }} />
                    <YAxis tick={{ fontSize: 11, fill: "#64748b" }} />
                    <Tooltip />
                    <Bar dataKey="user_count" fill="#2563eb" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          ) : (
            <div className="py-12 text-center text-xs text-slate-500">
              Chưa có bảng Gold Star Schema — hãy bấm <strong>Chạy Gold Star Schema ETL</strong>.
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
