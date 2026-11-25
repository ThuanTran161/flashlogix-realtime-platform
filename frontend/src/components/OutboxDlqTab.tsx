import { useEffect, useState } from "react";
import { AlertTriangle, MailCheck, RotateCcw } from "lucide-react";
import { apiRequest, fetchQuarantine, formatLocalDateTime } from "../api";
import type { OpsOverview, QuarantineResponse } from "../types";

interface Props {
  overview: OpsOverview | null;
  onRefresh: () => void;
}

export function OutboxDlqTab({ overview, onRefresh }: Props) {
  const [replaying, setReplaying] = useState(false);
  const [replayMsg, setReplayMsg] = useState<string | null>(null);
  const [quarantine, setQuarantine] = useState<QuarantineResponse | null>(null);
  const [selectedKey, setSelectedKey] = useState<string>("");

  async function loadQuarantine(key?: string) {
    const res = await fetchQuarantine(key || undefined);
    if (res.ok && res.data?.data) {
      setQuarantine(res.data.data);
      if (!key && res.data.data.objects.length > 0) {
        const firstKey = res.data.data.objects[0].key;
        setSelectedKey(firstKey);
        if (firstKey.endsWith(".json")) {
          const detail = await fetchQuarantine(firstKey);
          if (detail.ok && detail.data?.data) setQuarantine(detail.data.data);
        }
      }
    }
  }

  useEffect(() => {
    void loadQuarantine();
  }, []);

  async function handleReplay() {
    setReplaying(true);
    setReplayMsg(null);
    const res = await apiRequest<{ data: Record<string, unknown> }>(
      "/api/v1/orders/outbox/replay?limit=200",
      { method: "POST" }
    );
    if (res.ok) {
      setReplayMsg(`Replay complete: ${JSON.stringify(res.data.data)}`);
      onRefresh();
    } else {
      setReplayMsg(`Replay failed (${res.status}): ${JSON.stringify(res.data)}`);
    }
    setReplaying(false);
  }

  const counts = overview?.outbox.counts ?? { PUBLISHED: 0, PENDING: 0, FAILED: 0 };
  const rows = overview?.outbox.recent ?? [];

  return (
    <div className="space-y-5">
      {/* Outbox Summary + Replay */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-4">
        <div className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <div className="text-xs text-slate-500">PUBLISHED</div>
          <div className="mt-1 text-xl font-semibold text-emerald-600 num font-mono dark:text-emerald-400">
            {(counts.PUBLISHED ?? 0).toLocaleString()}
          </div>
        </div>
        <div className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <div className="text-xs text-slate-500">PENDING</div>
          <div className="mt-1 text-xl font-semibold text-amber-600 num font-mono dark:text-amber-400">
            {(counts.PENDING ?? 0).toLocaleString()}
          </div>
        </div>
        <div className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <div className="text-xs text-slate-500">FAILED</div>
          <div className="mt-1 text-xl font-semibold text-red-600 num font-mono dark:text-red-400">
            {(counts.FAILED ?? 0).toLocaleString()}
          </div>
        </div>
        <div className="flex items-center rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <button
            type="button"
            disabled={replaying}
            onClick={handleReplay}
            className="inline-flex w-full items-center justify-center gap-1.5 rounded-md bg-slate-900 px-3 py-2 text-xs font-medium text-white hover:bg-slate-800 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900"
          >
            <RotateCcw size={13} />
            {replaying ? "Đang replay..." : "Replay Outbox Events"}
          </button>
        </div>
      </div>

      {replayMsg && (
        <div className="rounded-md border border-slate-200 bg-white px-3 py-2 text-xs font-mono dark:border-zinc-800 dark:bg-zinc-900">
          {replayMsg}
        </div>
      )}

      {/* Outbox Table */}
      <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
        <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
          <MailCheck size={15} className="text-slate-500" />
          <span>Transactional Outbox Events (`outbox_events`)</span>
        </div>

        <div className="max-h-72 overflow-auto">
          <table className="w-full border-collapse text-left text-xs">
            <thead>
              <tr className="border-b border-slate-200 text-slate-500 dark:border-zinc-800 dark:text-zinc-400">
                <th className="py-2 pr-3 font-medium">Order ID</th>
                <th className="py-2 pr-3 font-medium">Outbox ID</th>
                <th className="py-2 pr-3 font-medium">Status</th>
                <th className="py-2 pr-3 font-medium text-right">Attempts</th>
                <th className="py-2 pr-3 font-medium">Last Error</th>
                <th className="py-2 font-medium">Published At</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-zinc-800/70">
              {rows.map((r) => (
                <tr key={r.id}>
                  <td className="py-1.5 pr-3 font-mono font-medium num">#{r.aggregate_id}</td>
                  <td className="py-1.5 pr-3 font-mono text-slate-500 num">#{r.id}</td>
                  <td className="py-1.5 pr-3">
                    <span
                      className={`rounded px-1.5 py-0.5 text-[10px] font-semibold ${
                        r.status === "PUBLISHED"
                          ? "bg-emerald-50 text-emerald-700 dark:bg-emerald-950/60 dark:text-emerald-300"
                          : "bg-amber-50 text-amber-700 dark:bg-amber-950/60 dark:text-amber-300"
                      }`}
                    >
                      {r.status}
                    </span>
                  </td>
                  <td className="py-1.5 pr-3 text-right font-mono num">{r.attempts}</td>
                  <td className="py-1.5 pr-3 font-mono text-[11px] text-slate-500">
                    {r.last_error ?? "-"}
                  </td>
                  <td className="py-1.5 font-mono text-[11px] text-slate-500">
                    {formatLocalDateTime(r.published_at)}
                  </td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td colSpan={6} className="py-4 text-center text-slate-400">
                    Chưa có Outbox event nào.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      {/* MinIO Quarantine Inspector */}
      <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
        <div className="mb-3 flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
            <AlertTriangle size={15} className="text-amber-500" />
            <span>Poison-Pill Quarantine / Kafka DLQ (MinIO)</span>
          </div>
          <button
            type="button"
            onClick={() => void loadQuarantine(selectedKey)}
            className="rounded border border-slate-200 px-2.5 py-1 text-xs hover:bg-slate-50 dark:border-zinc-700 dark:hover:bg-zinc-800"
          >
            Làm mới
          </button>
        </div>

        {quarantine && quarantine.objects.length > 0 ? (
          <div className="space-y-3">
            <select
              value={selectedKey}
              onChange={(e) => {
                setSelectedKey(e.target.value);
                void loadQuarantine(e.target.value);
              }}
              className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-xs font-mono dark:border-zinc-700 dark:bg-zinc-950"
            >
              {quarantine.objects.map((o) => (
                <option key={o.key} value={o.key}>
                  {o.key} ({o.size} bytes)
                </option>
              ))}
            </select>
            {quarantine.preview !== null && quarantine.preview !== undefined && (
              <pre className="max-h-56 overflow-auto rounded-md border border-slate-200 bg-slate-50 p-3 text-xs font-mono dark:border-zinc-800 dark:bg-zinc-950">
                {JSON.stringify(quarantine.preview, null, 2)}
              </pre>
            )}
          </div>
        ) : (
          <div className="text-xs text-slate-500">Chưa có poison-pill nào trong `quarantine/`.</div>
        )}
      </section>
    </div>
  );
}
