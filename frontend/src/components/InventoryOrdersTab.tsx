import { useEffect, useState } from "react";
import { CheckCircle2, Receipt, Search, Warehouse, XCircle } from "lucide-react";
import { fetchOrders, formatLocalDateTime } from "../api";
import type { InventoryItem, OrderRecord } from "../types";

interface Props {
  inventory: InventoryItem[];
}

export function InventoryOrdersTab({ inventory }: Props) {
  const [filterUid, setFilterUid] = useState("");
  const [filterPid, setFilterPid] = useState("");
  const [orders, setOrders] = useState<OrderRecord[]>([]);
  const [loading, setLoading] = useState(false);

  async function loadOrders() {
    setLoading(true);
    const pid = Number(filterPid);
    const res = await fetchOrders({
      user_id: filterUid || undefined,
      product_id: pid > 0 ? pid : undefined,
      limit: 50,
    });
    if (res.ok && Array.isArray(res.data?.data)) {
      setOrders(res.data.data);
    }
    setLoading(false);
  }

  useEffect(() => {
    void loadOrders();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="space-y-5">
      {/* Inventory Sync Table */}
      <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
        <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
          <Warehouse size={15} className="text-slate-500" />
          <span>Đối Soát Tồn Kho Realtime (PostgreSQL vs Redis RAM)</span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-left text-xs">
            <thead>
              <tr className="border-b border-slate-200 text-slate-500 dark:border-zinc-800 dark:text-zinc-400">
                <th className="py-2 pr-4 font-medium">ID</th>
                <th className="py-2 pr-4 font-medium">Product Name</th>
                <th className="py-2 pr-4 font-medium text-right">Price</th>
                <th className="py-2 pr-4 font-medium text-right">PostgreSQL</th>
                <th className="py-2 pr-4 font-medium text-right">Redis RAM</th>
                <th className="py-2 pr-4 font-medium text-right">Version</th>
                <th className="py-2 font-medium">Sync</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-zinc-800/70">
              {inventory.map((item) => (
                <tr key={item.id} className="hover:bg-slate-50/70 dark:hover:bg-zinc-800/40">
                  <td className="py-2 pr-4 font-mono num">#{item.id}</td>
                  <td className="py-2 pr-4 font-medium text-slate-800 dark:text-zinc-200">
                    {item.name}
                  </td>
                  <td className="py-2 pr-4 text-right font-mono num">${item.price.toFixed(2)}</td>
                  <td className="py-2 pr-4 text-right font-mono num">{item.db_stock}</td>
                  <td className="py-2 pr-4 text-right font-mono num">
                    {item.redis_stock ?? "N/A"}
                  </td>
                  <td className="py-2 pr-4 text-right font-mono num">v{item.version}</td>
                  <td className="py-2">
                    {item.in_sync ? (
                      <span className="inline-flex items-center gap-1 text-emerald-600 dark:text-emerald-400">
                        <CheckCircle2 size={13} /> In Sync
                      </span>
                    ) : (
                      <span className="inline-flex items-center gap-1 text-amber-600 dark:text-amber-400">
                        <XCircle size={13} /> Drift
                      </span>
                    )}
                  </td>
                </tr>
              ))}
              {inventory.length === 0 && (
                <tr>
                  <td colSpan={7} className="py-4 text-center text-slate-400">
                    Chưa có sản phẩm trong OLTP PostgreSQL.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      {/* Recent Orders */}
      <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
            <Receipt size={15} className="text-slate-500" />
            <span>Lịch Sử Đơn Hàng OLTP (`GET /api/v1/orders`)</span>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <input
              type="text"
              value={filterUid}
              onChange={(e) => setFilterUid(e.target.value)}
              placeholder="Lọc user_id..."
              className="rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-mono dark:border-zinc-700 dark:bg-zinc-950"
            />
            <input
              type="number"
              min={0}
              value={filterPid}
              onChange={(e) => setFilterPid(e.target.value)}
              placeholder="Product ID"
              className="w-28 rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-xs font-mono num dark:border-zinc-700 dark:bg-zinc-950"
            />
            <button
              type="button"
              onClick={loadOrders}
              disabled={loading}
              className="inline-flex items-center gap-1 rounded-md bg-slate-900 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-800 dark:bg-zinc-100 dark:text-zinc-900"
            >
              <Search size={12} />
              {loading ? "Đang tải..." : "Tìm"}
            </button>
          </div>
        </div>

        <div className="max-h-96 overflow-auto">
          <table className="w-full border-collapse text-left text-xs">
            <thead>
              <tr className="border-b border-slate-200 text-slate-500 dark:border-zinc-800 dark:text-zinc-400">
                <th className="py-2 pr-3 font-medium">Order ID</th>
                <th className="py-2 pr-3 font-medium">User ID</th>
                <th className="py-2 pr-3 font-medium text-right">Product</th>
                <th className="py-2 pr-3 font-medium text-right">Qty</th>
                <th className="py-2 pr-3 font-medium text-right">Amount</th>
                <th className="py-2 pr-3 font-medium text-right">Latency</th>
                <th className="py-2 pr-3 font-medium">Status</th>
                <th className="py-2 font-medium">Created At</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-zinc-800/70">
              {orders.map((o) => (
                <tr key={o.id} className="hover:bg-slate-50/70 dark:hover:bg-zinc-800/40">
                  <td className="py-1.5 pr-3 font-mono num">#{o.id}</td>
                  <td className="py-1.5 pr-3 font-mono">{o.user_id}</td>
                  <td className="py-1.5 pr-3 text-right font-mono num">#{o.product_id}</td>
                  <td className="py-1.5 pr-3 text-right font-mono num">{o.quantity}</td>
                  <td className="py-1.5 pr-3 text-right font-mono num">
                    ${o.total_amount.toFixed(2)}
                  </td>
                  <td className="py-1.5 pr-3 text-right font-mono num">
                    {o.latency_ms.toFixed(1)} ms
                  </td>
                  <td className="py-1.5 pr-3">
                    <span className="rounded border border-emerald-200 bg-emerald-50 px-1.5 py-0.5 text-[10px] font-medium text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/60 dark:text-emerald-300">
                      {o.status}
                    </span>
                  </td>
                  <td className="py-1.5 font-mono text-[11px] text-slate-500">
                    {formatLocalDateTime(o.created_at)}
                  </td>
                </tr>
              ))}
              {orders.length === 0 && (
                <tr>
                  <td colSpan={8} className="py-4 text-center text-slate-400">
                    Không tìm thấy đơn hàng phù hợp.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
