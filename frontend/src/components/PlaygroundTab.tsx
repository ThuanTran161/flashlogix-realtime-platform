import { useState } from "react";
import {
  KeyRound,
  PackagePlus,
  RefreshCw,
  Send,
  ShieldAlert,
  Sliders,
  Zap,
} from "lucide-react";
import { apiRequest, generateKey } from "../api";
import type { ApiResult, InventoryItem } from "../types";

interface Props {
  inventory: InventoryItem[];
  onRefresh: () => void;
}

interface BurstResult {
  total: number;
  wallSec: number;
  rps: number;
  ok201: number;
  oos400: number;
  conflict409: number;
  other: number;
  avgMs: number;
}

export function PlaygroundTab({ inventory, onRefresh }: Props) {
  const [selectedPid, setSelectedPid] = useState<number>(inventory[0]?.id ?? 1);
  const [resetStockVal, setResetStockVal] = useState<number>(100);
  const [stockMsg, setStockMsg] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("FlashLogix Gaming Mouse X1");
  const [newPrice, setNewPrice] = useState(79.99);
  const [newStock, setNewStock] = useState(50);

  const [orderPid, setOrderPid] = useState<number>(inventory[0]?.id ?? 1);
  const [orderQty, setOrderQty] = useState<number>(1);
  const [orderUid, setOrderUid] = useState("demo_shopper_01");
  const [idemKey, setIdemKey] = useState(() => generateKey("ui"));
  const [orderResult, setOrderResult] = useState<ApiResult | null>(null);
  const [orderLoading, setOrderLoading] = useState(false);

  const [burstTotal, setBurstTotal] = useState(40);
  const [burstConcurrency, setBurstConcurrency] = useState(15);
  const [burstProgress, setBurstProgress] = useState(0);
  const [burstRunning, setBurstRunning] = useState(false);
  const [burstResult, setBurstResult] = useState<BurstResult | null>(null);

  const [botUid, setBotUid] = useState("bot_attacker_999");
  const [botCodes, setBotCodes] = useState<number[] | null>(null);
  const [botRunning, setBotRunning] = useState(false);

  async function handleResetStock() {
    setStockMsg(null);
    const res = await apiRequest(`/api/v1/products/${selectedPid}/init-stock`, {
      method: "POST",
      body: { initial_stock: resetStockVal },
    });
    if (res.ok) {
      setStockMsg(`Updated Product #${selectedPid} stock = ${resetStockVal} (${res.elapsedMs} ms)`);
      onRefresh();
    } else {
      setStockMsg(`Error (${res.status}): ${JSON.stringify(res.data)}`);
    }
  }

  async function handleCreateProduct() {
    const res = await apiRequest<{ data: { id: number } }>("/api/v1/products", {
      method: "POST",
      body: { name: newName, price: newPrice, stock: newStock },
    });
    if (res.ok) {
      const createdId = res.data.data.id;
      setSelectedPid(createdId);
      setOrderPid(createdId);
      setStockMsg(`Created Product #${createdId} (${newName})`);
      setShowCreate(false);
      onRefresh();
    } else {
      setStockMsg(`Create failed (${res.status}): ${JSON.stringify(res.data)}`);
    }
  }

  async function handleSendOrder() {
    setOrderLoading(true);
    const res = await apiRequest("/api/v1/orders", {
      method: "POST",
      body: { user_id: orderUid, product_id: orderPid, quantity: orderQty },
      headers: idemKey.trim() ? { "Idempotency-Key": idemKey.trim() } : {},
    });
    setOrderResult(res);
    setOrderLoading(false);
    onRefresh();
  }

  async function handleBurst() {
    setBurstRunning(true);
    setBurstProgress(0);
    setBurstResult(null);

    const t0 = performance.now();
    const codes: number[] = [];
    const latencies: number[] = [];
    let nextIdx = 0;

    async function worker() {
      while (nextIdx < burstTotal) {
        const idx = nextIdx++;
        const r = await apiRequest("/api/v1/orders", {
          method: "POST",
          body: { user_id: `burst_user_${idx}`, product_id: selectedPid, quantity: 1 },
          headers: { "Idempotency-Key": generateKey(`burst_${idx}`) },
        });
        codes.push(r.status);
        latencies.push(r.elapsedMs);
        setBurstProgress(codes.length);
      }
    }

    const workers = Array.from({ length: Math.min(burstConcurrency, burstTotal) }, () => worker());
    await Promise.all(workers);

    const wallSec = Math.max(0.01, (performance.now() - t0) / 1000);
    setBurstResult({
      total: burstTotal,
      wallSec: Math.round(wallSec * 100) / 100,
      rps: Math.round((burstTotal / wallSec) * 10) / 10,
      ok201: codes.filter((c) => c === 201).length,
      oos400: codes.filter((c) => c === 400).length,
      conflict409: codes.filter((c) => c === 409).length,
      other: codes.filter((c) => ![201, 400, 409].includes(c)).length,
      avgMs: Math.round((latencies.reduce((a, b) => a + b, 0) / latencies.length) * 10) / 10,
    });
    setBurstRunning(false);
    onRefresh();
  }

  async function handleBotAttack() {
    setBotRunning(true);
    const out: number[] = [];
    for (let i = 0; i < 6; i++) {
      const r = await apiRequest("/api/v1/orders", {
        method: "POST",
        body: { user_id: botUid, product_id: selectedPid, quantity: 1 },
        headers: { "Idempotency-Key": generateKey(`bot_${i}`) },
      });
      out.push(r.status);
    }
    setBotCodes(out);
    setBotRunning(false);
    onRefresh();
  }

  return (
    <div className="space-y-5">
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {/* Inventory Management */}
        <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
              <Sliders size={15} className="text-slate-500" />
              Quản lý Kho & Sản phẩm
            </h2>
            <button
              type="button"
              onClick={() => setShowCreate((v) => !v)}
              className="inline-flex items-center gap-1.5 rounded-md border border-slate-200 px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-zinc-700 dark:text-zinc-200 dark:hover:bg-zinc-800"
            >
              <PackagePlus size={13} />
              {showCreate ? "Đóng" : "Tạo sản phẩm"}
            </button>
          </div>

          <div className="space-y-3">
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-zinc-400">
                Chọn sản phẩm
              </label>
              <select
                value={selectedPid}
                onChange={(e) => {
                  const v = Number(e.target.value);
                  setSelectedPid(v);
                  setOrderPid(v);
                }}
                className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-950"
              >
                {inventory.map((p) => (
                  <option key={p.id} value={p.id}>
                    #{p.id} — {p.name} (DB={p.db_stock}, Redis={p.redis_stock ?? "N/A"})
                  </option>
                ))}
                {inventory.length === 0 && <option value={1}>#1 (No products yet)</option>}
              </select>
            </div>

            <div className="flex items-end gap-2">
              <div className="flex-1">
                <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-zinc-400">
                  Số lượng tồn kho mới
                </label>
                <input
                  type="number"
                  min={0}
                  value={resetStockVal}
                  onChange={(e) => setResetStockVal(Number(e.target.value))}
                  className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm font-mono num dark:border-zinc-700 dark:bg-zinc-950"
                />
              </div>
              <button
                type="button"
                onClick={handleResetStock}
                className="inline-flex items-center gap-1.5 rounded-md bg-slate-900 px-3.5 py-2 text-xs font-medium text-white hover:bg-slate-800 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-200"
              >
                <RefreshCw size={13} />
                Nạp / Reset Kho
              </button>
            </div>

            {stockMsg && (
              <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700 dark:border-zinc-800 dark:bg-zinc-950 dark:text-zinc-300">
                {stockMsg}
              </div>
            )}

            {showCreate && (
              <div className="space-y-2.5 rounded-md border border-slate-200 bg-slate-50 p-3 dark:border-zinc-800 dark:bg-zinc-950">
                <div className="text-xs font-semibold text-slate-700 dark:text-zinc-200">
                  Thêm sản phẩm mới
                </div>
                <input
                  type="text"
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  placeholder="Tên sản phẩm"
                  className="w-full rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-sm dark:border-zinc-700 dark:bg-zinc-900"
                />
                <div className="grid grid-cols-2 gap-2">
                  <input
                    type="number"
                    min={1}
                    step={0.01}
                    value={newPrice}
                    onChange={(e) => setNewPrice(Number(e.target.value))}
                    className="rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-sm font-mono num dark:border-zinc-700 dark:bg-zinc-900"
                  />
                  <input
                    type="number"
                    min={0}
                    value={newStock}
                    onChange={(e) => setNewStock(Number(e.target.value))}
                    className="rounded-md border border-slate-300 bg-white px-2.5 py-1.5 text-sm font-mono num dark:border-zinc-700 dark:bg-zinc-900"
                  />
                </div>
                <button
                  type="button"
                  onClick={handleCreateProduct}
                  className="inline-flex items-center gap-1.5 rounded-md bg-blue-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-blue-700"
                >
                  <PackagePlus size={13} />
                  Tạo sản phẩm
                </button>
              </div>
            )}
          </div>
        </section>

        {/* Direct Order + Idempotency Tester */}
        <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
            <Send size={15} className="text-slate-500" />
            Đặt Đơn Trực Tiếp & Test Idempotency
          </h2>

          <div className="space-y-3">
            <div className="grid grid-cols-3 gap-2">
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-zinc-400">
                  Product ID
                </label>
                <input
                  type="number"
                  min={1}
                  value={orderPid}
                  onChange={(e) => setOrderPid(Number(e.target.value))}
                  className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm font-mono num dark:border-zinc-700 dark:bg-zinc-950"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-zinc-400">
                  Quantity (1-10)
                </label>
                <input
                  type="number"
                  min={1}
                  max={10}
                  value={orderQty}
                  onChange={(e) => setOrderQty(Number(e.target.value))}
                  className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm font-mono num dark:border-zinc-700 dark:bg-zinc-950"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-zinc-400">
                  User ID
                </label>
                <input
                  type="text"
                  value={orderUid}
                  onChange={(e) => setOrderUid(e.target.value)}
                  className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm font-mono dark:border-zinc-700 dark:bg-zinc-950"
                />
              </div>
            </div>

            <div className="flex items-end gap-2">
              <div className="flex-1">
                <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-zinc-400">
                  Idempotency-Key (giữ nguyên để test chống trừ kho kép)
                </label>
                <input
                  type="text"
                  value={idemKey}
                  onChange={(e) => setIdemKey(e.target.value)}
                  className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm font-mono dark:border-zinc-700 dark:bg-zinc-950"
                />
              </div>
              <button
                type="button"
                onClick={() => setIdemKey(generateKey("ui"))}
                className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 px-3 py-2 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-zinc-700 dark:text-zinc-200 dark:hover:bg-zinc-800"
              >
                <KeyRound size={13} />
                Key mới
              </button>
            </div>

            <button
              type="button"
              disabled={orderLoading}
              onClick={handleSendOrder}
              className="inline-flex w-full items-center justify-center gap-2 rounded-md bg-blue-600 px-4 py-2 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
            >
              <Send size={13} />
              {orderLoading ? "Đang gửi..." : "Gửi Đơn Hàng (POST /api/v1/orders)"}
            </button>

            {orderResult && (
              <div className="space-y-1.5 rounded-md border border-slate-200 bg-slate-50 p-3 dark:border-zinc-800 dark:bg-zinc-950">
                <div className="flex flex-wrap items-center justify-between gap-2 text-xs font-mono">
                  <span
                    className={`font-semibold ${
                      orderResult.ok
                        ? "text-emerald-600 dark:text-emerald-400"
                        : "text-amber-600 dark:text-amber-400"
                    }`}
                  >
                    HTTP {orderResult.status}
                  </span>
                  <span className="text-slate-500 dark:text-zinc-400">
                    Server: {orderResult.processTimeMs ?? orderResult.elapsedMs} ms | Request-ID:{" "}
                    {orderResult.requestId ?? "N/A"}
                  </span>
                </div>
                <pre className="max-h-40 overflow-auto text-[11px] text-slate-700 dark:text-zinc-300">
                  {JSON.stringify(orderResult.data, null, 2)}
                </pre>
              </div>
            )}
          </div>
        </section>
      </div>

      {/* Load & Fraud Simulation */}
      <section className="rounded-lg border border-slate-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
        <h2 className="mb-3 flex items-center gap-2 text-sm font-semibold text-slate-800 dark:text-zinc-100">
          <Zap size={15} className="text-slate-500" />
          Giả Lập Tải Cao & Bot Velocity Fraud
        </h2>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <div className="space-y-3 rounded-md border border-slate-200 p-3 dark:border-zinc-800">
            <div className="text-xs font-semibold text-slate-700 dark:text-zinc-200">
              1. Concurrent Burst Test (Product #{selectedPid})
            </div>
            <div className="grid grid-cols-2 gap-3 text-xs">
              <label className="space-y-1">
                <span className="text-slate-500 dark:text-zinc-400">
                  Tổng requests: <strong className="font-mono">{burstTotal}</strong>
                </span>
                <input
                  type="range"
                  min={10}
                  max={150}
                  step={10}
                  value={burstTotal}
                  onChange={(e) => setBurstTotal(Number(e.target.value))}
                  className="w-full"
                />
              </label>
              <label className="space-y-1">
                <span className="text-slate-500 dark:text-zinc-400">
                  Đồng thời: <strong className="font-mono">{burstConcurrency}</strong>
                </span>
                <input
                  type="range"
                  min={2}
                  max={30}
                  value={burstConcurrency}
                  onChange={(e) => setBurstConcurrency(Number(e.target.value))}
                  className="w-full"
                />
              </label>
            </div>

            <button
              type="button"
              disabled={burstRunning}
              onClick={handleBurst}
              className="inline-flex w-full items-center justify-center gap-1.5 rounded-md border border-slate-300 bg-slate-900 px-3 py-2 text-xs font-medium text-white hover:bg-slate-800 disabled:opacity-50 dark:border-zinc-700 dark:bg-zinc-100 dark:text-zinc-900"
            >
              <Zap size={13} />
              {burstRunning
                ? `Đang chạy (${burstProgress}/${burstTotal})...`
                : "Chạy Burst Flash Sale"}
            </button>

            {burstRunning && (
              <div className="h-1.5 w-full overflow-hidden rounded-full bg-slate-200 dark:bg-zinc-800">
                <div
                  className="h-full bg-blue-600 transition-all"
                  style={{ width: `${Math.round((burstProgress / burstTotal) * 100)}%` }}
                />
              </div>
            )}

            {burstResult && (
              <div className="grid grid-cols-4 gap-2 rounded bg-slate-50 p-2.5 text-center text-xs font-mono num dark:bg-zinc-950">
                <div>
                  <div className="text-[10px] text-slate-400">RPS</div>
                  <div className="font-semibold">{burstResult.rps}/s</div>
                </div>
                <div>
                  <div className="text-[10px] text-slate-400">201 OK</div>
                  <div className="font-semibold text-emerald-600">{burstResult.ok201}</div>
                </div>
                <div>
                  <div className="text-[10px] text-slate-400">400 OOS</div>
                  <div className="font-semibold text-amber-600">{burstResult.oos400}</div>
                </div>
                <div>
                  <div className="text-[10px] text-slate-400">Avg Lat</div>
                  <div className="font-semibold">{burstResult.avgMs}ms</div>
                </div>
              </div>
            )}
          </div>

          <div className="space-y-3 rounded-md border border-slate-200 p-3 dark:border-zinc-800">
            <div className="text-xs font-semibold text-slate-700 dark:text-zinc-200">
              2. Bot Velocity Fraud Test (&ge; 5 đơn/phút)
            </div>
            <div>
              <label className="mb-1 block text-xs text-slate-500 dark:text-zinc-400">
                Bot User ID
              </label>
              <input
                type="text"
                value={botUid}
                onChange={(e) => setBotUid(e.target.value)}
                className="w-full rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm font-mono dark:border-zinc-700 dark:bg-zinc-950"
              />
            </div>
            <button
              type="button"
              disabled={botRunning}
              onClick={handleBotAttack}
              className="inline-flex w-full items-center justify-center gap-1.5 rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-xs font-medium text-amber-800 hover:bg-amber-100 disabled:opacity-50 dark:border-amber-800 dark:bg-amber-950/50 dark:text-amber-300"
            >
              <ShieldAlert size={13} />
              {botRunning ? "Đang gửi 6 đơn..." : "Gửi 6 Đơn Bot Liên Tiếp"}
            </button>
            {botCodes && (
              <div className="rounded bg-slate-50 px-3 py-2 text-xs text-slate-600 dark:bg-zinc-950 dark:text-zinc-300">
                HTTP Codes: <span className="font-mono">{JSON.stringify(botCodes)}</span> — Qua tab{" "}
                <strong>Lakehouse & ETL</strong> bấm chạy Gold ETL để kiểm tra cờ{" "}
                <code>is_fraudulent</code>.
              </div>
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
