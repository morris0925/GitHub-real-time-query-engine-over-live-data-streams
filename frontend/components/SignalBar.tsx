"use client";

/**
 * Dev Pipeline Signal bar (design proposal §3).
 *
 * - A status BAR with teal chrome — visually distinct from both the
 *   red/orange/yellow severity badges and the violet AI accent.
 * - Its three components (CI stability / PR velocity / commit cadence) are
 *   shown side by side, never collapsed into one number, so "CI is actually
 *   failing" and "everyone's on vacation" stay distinguishable.
 * - Permanent honesty caption: this is built from Tier 1 dev-process
 *   signals, not production telemetry.
 */

import { useEffect, useState } from "react";
import { fetchSignal, PipelineSignal, SignalComponent, SignalStatus } from "@/lib/api";

const STATUS_STYLES: Record<SignalStatus, { dot: string; label: string }> = {
  ok: { dot: "bg-emerald-400", label: "OK" },
  warn: { dot: "bg-amber-400", label: "Degraded" },
  alert: { dot: "bg-red-500", label: "Alert" },
  unknown: { dot: "bg-slate-600", label: "No data" },
};

function formatDetail(name: string, component: SignalComponent): string {
  if (component.status === "unknown") return "insufficient data";
  if (name === "CI stability" && typeof component.failure_rate === "number")
    return `${Math.round(component.failure_rate * 100)}% failure rate`;
  if (name === "PR velocity" && typeof component.avg_merge_hours === "number")
    return `${component.avg_merge_hours.toFixed(1)}h avg merge`;
  if (name === "Commit cadence" && typeof component.pushes_per_hour === "number")
    return `${component.pushes_per_hour.toFixed(1)} pushes/h`;
  return "";
}

function SignalCell({ name, component }: { name: string; component: SignalComponent }) {
  const style = STATUS_STYLES[component.status] ?? STATUS_STYLES.unknown;
  return (
    <div className="flex flex-1 items-center gap-3 px-4 py-2">
      <span className={`h-2.5 w-2.5 shrink-0 rounded-full ${style.dot}`} />
      <div className="min-w-0">
        <div className="text-xs font-medium uppercase tracking-wide text-slate-400">
          {name}
        </div>
        <div className="truncate text-sm text-slate-200">
          {style.label}
          {formatDetail(name, component) && (
            <span className="text-slate-400"> · {formatDetail(name, component)}</span>
          )}
        </div>
      </div>
    </div>
  );
}

export default function SignalBar({ pollMs = 10000 }: { pollMs?: number }) {
  const [signal, setSignal] = useState<PipelineSignal | null>(null);

  useEffect(() => {
    let active = true;
    const load = () =>
      fetchSignal()
        .then((s) => active && setSignal(s))
        .catch(() => active && setSignal(null));
    load();
    const timer = setInterval(load, pollMs);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [pollMs]);

  return (
    <section
      aria-label="Dev Pipeline Signal"
      className="rounded-lg border border-teal-800/60 bg-slate-900/60"
      style={{ borderLeftColor: "var(--signal-accent)", borderLeftWidth: 3 }}
    >
      <div className="flex items-center justify-between border-b border-slate-800 px-4 py-1.5">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-teal-400">
          Dev Pipeline Signal
        </h2>
        <span className="text-[11px] text-slate-500">
          {signal?.caption ??
            "Based on CI/PR/commit signals — not a live production health check"}
        </span>
      </div>
      <div className="flex divide-x divide-slate-800">
        <SignalCell
          name="CI stability"
          component={signal?.ci_stability ?? { status: "unknown" }}
        />
        <SignalCell
          name="PR velocity"
          component={signal?.pr_velocity ?? { status: "unknown" }}
        />
        <SignalCell
          name="Commit cadence"
          component={signal?.commit_cadence ?? { status: "unknown" }}
        />
      </div>
    </section>
  );
}
