import { useEffect, useState } from "react";
import { ShieldCheck } from "lucide-react";
import { fetchDataQuality } from "../api";
import type { DataQualityResponse } from "../types";

export function DataQualityTab() {
  const [dqState, setDqState] = useState<DataQualityResponse | null>(null);

  async function loadDq() {
    const res = await fetchDataQuality();
    if (res.ok && res.data?.data) {
      setDqState(res.data.data);
    }
  }

  useEffect(() => {
    void loadDq();
  }, []);

  const r = dqState?.report as Record<string, unknown> | null;

  return (
    <div className="space-y-4">
      <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
        <div className="mb-4 flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
            <ShieldCheck size={15} className="text-emerald-600" />
            <span>Data Quality & Lakehouse Reconciliation SLA</span>
          </div>
          <button
            type="button"
            onClick={() => void loadDq()}
            className="rounded border border-slate-200 px-2.5 py-1 text-xs hover:bg-slate-50 dark:border-zinc-700 dark:hover:bg-zinc-800"
          >
            Làm mới
          </button>
        </div>

        {dqState?.available && r ? (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
              <MetricBox
                label="OLTP ↔ Gold Parity"
                value={`${Number(r.order_parity_pct ?? 0).toFixed(2)}%`}
              />
              <MetricBox label="Revenue Delta" value={`$${Number(r.revenue_delta ?? 0).toFixed(2)}`} />
              <MetricBox
                label="Duplicates Filtered"
                value={Number(r.duplicates_filtered ?? 0).toLocaleString()}
              />
              <MetricBox
                label="Quarantined / DLQ"
                value={Number(r.quarantined_objects ?? 0).toLocaleString()}
              />
              <MetricBox
                label="PII Compliance"
                value={r.pii_compliance_passed ? "PASS (0 violations)" : "FAIL"}
              />
            </div>

            <pre className="max-h-96 overflow-auto rounded-md border border-slate-200 bg-slate-50 p-3 text-xs font-mono dark:border-zinc-800 dark:bg-zinc-950">
              {JSON.stringify(r, null, 2)}
            </pre>
          </div>
        ) : (
          <div className="py-10 text-center text-xs text-slate-500">
            Chưa có báo cáo Data Quality. Hãy qua tab <strong>Lakehouse & ETL</strong> và bấm chạy Gold ETL.
          </div>
        )}
      </section>
    </div>
  );
}

function MetricBox({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-slate-100 bg-slate-50 p-3 dark:border-zinc-800 dark:bg-zinc-950">
      <div className="text-[11px] text-slate-500">{label}</div>
      <div className="mt-1 font-mono text-base font-semibold num text-slate-900 dark:text-zinc-100">
        {value}
      </div>
    </div>
  );
}
