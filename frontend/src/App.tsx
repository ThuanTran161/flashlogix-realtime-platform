import { useCallback, useEffect, useState } from "react";
import {
  Activity,
  Database,
  ExternalLink,
  MailCheck,
  Moon,
  Package,
  Play,
  RefreshCw,
  Server,
  ShieldCheck,
  Sun,
} from "lucide-react";
import { fetchHealth, fetchMetrics, fetchOverview } from "./api";
import { DataQualityTab } from "./components/DataQualityTab";
import { InventoryOrdersTab } from "./components/InventoryOrdersTab";
import { LakehouseTab } from "./components/LakehouseTab";
import { OutboxDlqTab } from "./components/OutboxDlqTab";
import { OverviewTab } from "./components/OverviewTab";
import { PlaygroundTab } from "./components/PlaygroundTab";
import type { HealthResponse, MetricsResponse, OpsOverview } from "./types";

type TabKey = "overview" | "playground" | "inventory" | "outbox" | "lakehouse" | "dq";

const NAV_ITEMS: Array<{ id: TabKey; label: string; icon: typeof Activity }> = [
  { id: "overview", label: "Overview & Metrics", icon: Activity },
  { id: "playground", label: "API Playground", icon: Play },
  { id: "inventory", label: "Inventory & Orders", icon: Package },
  { id: "outbox", label: "Outbox & DLQ", icon: MailCheck },
  { id: "lakehouse", label: "Lakehouse & ETL", icon: Database },
  { id: "dq", label: "Data Quality SLA", icon: ShieldCheck },
];

export default function App() {
  const [activeTab, setActiveTab] = useState<TabKey>("overview");
  const [dark, setDark] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [metrics, setMetrics] = useState<MetricsResponse | null>(null);
  const [overview, setOverview] = useState<OpsOverview | null>(null);
  const [loading, setLoading] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<string>("-");

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
  }, [dark]);

  const refreshAll = useCallback(async () => {
    setLoading(true);
    const [h, m, o] = await Promise.all([fetchHealth(), fetchMetrics(), fetchOverview()]);
    if (h.ok) setHealth(h.data);
    else setHealth(null);
    if (m.ok) setMetrics(m.data);
    if (o.ok && o.data?.data) setOverview(o.data.data);
    setLastUpdated(new Date().toLocaleTimeString());
    setLoading(false);
  }, []);

  useEffect(() => {
    void refreshAll();
  }, [refreshAll]);

  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(() => {
      void refreshAll();
    }, 3000);
    return () => clearInterval(id);
  }, [autoRefresh, refreshAll]);

  const svc = health?.services;

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 dark:bg-zinc-950 dark:text-zinc-100">
      <div className="flex min-h-screen flex-col md:flex-row">
        {/* Sidebar */}
        <aside className="flex w-full shrink-0 flex-col justify-between border-b border-slate-200 bg-white p-4 md:w-60 md:border-r md:border-b-0 dark:border-zinc-800 dark:bg-zinc-900">
          <div>
            <div className="mb-5 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className="flex h-7 w-7 items-center justify-center rounded-md bg-slate-900 text-white dark:bg-zinc-100 dark:text-zinc-900">
                  <Server size={15} strokeWidth={2} />
                </span>
                <div>
                  <div className="text-sm font-semibold leading-none tracking-tight">FlashLogix</div>
                  <div className="mt-0.5 text-[11px] text-slate-400">Ops & Lakehouse</div>
                </div>
              </div>
              <button
                type="button"
                aria-label="Toggle theme"
                onClick={() => setDark((d) => !d)}
                className="rounded-md border border-slate-200 p-1.5 text-slate-500 hover:bg-slate-100 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-800"
              >
                {dark ? <Sun size={14} /> : <Moon size={14} />}
              </button>
            </div>

            <nav className="space-y-1">
              {NAV_ITEMS.map((item) => {
                const Icon = item.icon;
                const active = activeTab === item.id;
                return (
                  <button
                    key={item.id}
                    type="button"
                    onClick={() => setActiveTab(item.id)}
                    className={`flex w-full items-center gap-2.5 rounded-md px-2.5 py-2 text-left text-xs font-medium transition-colors ${
                      active
                        ? "bg-slate-900 text-white dark:bg-zinc-100 dark:text-zinc-900"
                        : "text-slate-600 hover:bg-slate-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
                    }`}
                  >
                    <Icon size={15} strokeWidth={1.8} />
                    <span>{item.label}</span>
                  </button>
                );
              })}
            </nav>
          </div>

          <div className="mt-6 space-y-2 border-t border-slate-200 pt-4 text-xs dark:border-zinc-800">
            <div className="font-medium text-slate-400">Endpoints</div>
            <a
              href="/docs"
              target="_blank"
              rel="noreferrer"
              className="flex items-center justify-between text-slate-600 hover:text-slate-900 dark:text-zinc-400 dark:hover:text-zinc-100"
            >
              <span>Swagger Docs</span>
              <ExternalLink size={12} />
            </a>
            <a
              href="/metrics"
              target="_blank"
              rel="noreferrer"
              className="flex items-center justify-between text-slate-600 hover:text-slate-900 dark:text-zinc-400 dark:hover:text-zinc-100"
            >
              <span>Telemetry JSON</span>
              <ExternalLink size={12} />
            </a>
            <a
              href="http://localhost:9001"
              target="_blank"
              rel="noreferrer"
              className="flex items-center justify-between text-slate-600 hover:text-slate-900 dark:text-zinc-400 dark:hover:text-zinc-100"
            >
              <span>MinIO Console</span>
              <ExternalLink size={12} />
            </a>
          </div>
        </aside>

        {/* Main Content */}
        <main className="flex-1 overflow-x-hidden p-4 md:p-6">
          {/* Top Health & Control Bar */}
          <header className="mb-5 flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 pb-4 dark:border-zinc-800">
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge label="FastAPI" healthy={health !== null} />
              <StatusBadge label="PostgreSQL" healthy={svc?.database === "HEALTHY"} />
              <StatusBadge label="Redis" healthy={svc?.redis === "HEALTHY"} />
              <StatusBadge label="Kafka" healthy={svc?.kafka === "HEALTHY"} />
              <StatusBadge label="MinIO" healthy={svc?.minio === "HEALTHY"} />
            </div>

            <div className="flex items-center gap-3">
              <label className="flex cursor-pointer items-center gap-1.5 text-xs text-slate-500 dark:text-zinc-400">
                <input
                  type="checkbox"
                  checked={autoRefresh}
                  onChange={(e) => setAutoRefresh(e.target.checked)}
                  className="rounded border-slate-300"
                />
                Auto (3s)
              </label>
              <span className="font-mono text-[11px] text-slate-400">{lastUpdated}</span>
              <button
                type="button"
                onClick={() => void refreshAll()}
                className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-200 dark:hover:bg-zinc-800"
              >
                <RefreshCw size={12} className={loading ? "animate-spin" : ""} />
                Refresh
              </button>
            </div>
          </header>

          {activeTab === "overview" && <OverviewTab overview={overview} metrics={metrics} />}
          {activeTab === "playground" && (
            <PlaygroundTab
              inventory={overview?.inventory ?? []}
              onRefresh={() => void refreshAll()}
            />
          )}
          {activeTab === "inventory" && (
            <InventoryOrdersTab inventory={overview?.inventory ?? []} />
          )}
          {activeTab === "outbox" && (
            <OutboxDlqTab overview={overview} onRefresh={() => void refreshAll()} />
          )}
          {activeTab === "lakehouse" && (
            <LakehouseTab overview={overview} onRefresh={() => void refreshAll()} />
          )}
          {activeTab === "dq" && <DataQualityTab />}
        </main>
      </div>
    </div>
  );
}

function StatusBadge({ label, healthy }: { label: string; healthy: boolean }) {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-md border border-slate-200 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 dark:border-zinc-800 dark:bg-zinc-900 dark:text-zinc-300">
      <span
        className={`h-1.5 w-1.5 rounded-full ${
          healthy ? "bg-emerald-500" : "bg-red-500"
        }`}
      />
      {label}
    </span>
  );
}
