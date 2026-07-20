"use client";

/**
 * Incident feed (left column) — passive-notification mode (§1).
 * Polls GET /anomalies every `pollMs`; new anomalies appear as cards.
 */

import { Anomaly } from "@/lib/api";
import { SeverityBadge } from "@/components/badges";

function timeAgo(iso: string): string {
  const seconds = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

const TYPE_LABELS: Record<string, string> = {
  ci_failure_spike: "CI failure spike",
  merge_time_anomaly: "Merge time anomaly",
  commit_drought: "Commit drought",
};

export default function IncidentFeed({
  anomalies,
  selectedId,
  onSelect,
  error,
}: {
  anomalies: Anomaly[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  error: string | null;
}) {
  return (
    <section
      aria-label="Incident feed"
      className="flex min-h-0 flex-col rounded-lg border border-slate-800 bg-slate-900/60"
    >
      <div className="flex items-center justify-between border-b border-slate-800 px-4 py-2">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400">
          Incident feed
        </h2>
        <span className="text-[11px] text-slate-500">
          {error ? "API unreachable" : `${anomalies.length} anomalies`}
        </span>
      </div>

      <div className="flex-1 space-y-2 overflow-y-auto p-3">
        {anomalies.length === 0 && (
          <p className="px-1 py-6 text-center text-sm text-slate-500">
            {error
              ? "Cannot reach the diagnostic API. Is uvicorn running on :8000?"
              : "No anomalies detected. Use “Trigger demo anomaly” to seed one."}
          </p>
        )}

        {anomalies.map((anomaly) => {
          const selected = anomaly.anomaly_id === selectedId;
          return (
            <button
              key={anomaly.anomaly_id}
              onClick={() => onSelect(anomaly.anomaly_id)}
              className={`w-full rounded-md border p-3 text-left transition-colors ${
                selected
                  ? "border-slate-500 bg-slate-800"
                  : "border-slate-800 bg-slate-900 hover:border-slate-600"
              }`}
            >
              <div className="mb-1 flex items-center gap-2">
                <SeverityBadge severity={anomaly.severity} />
                {anomaly.is_demo && (
                  <span
                    className="rounded border border-slate-600 px-1.5 py-0.5 text-[10px] uppercase text-slate-400"
                    title="Manually captured live-CI snapshot (real data), not an auto-detected anomaly"
                  >
                    snapshot
                  </span>
                )}
                <span className="ml-auto text-[11px] text-slate-500">
                  {timeAgo(anomaly.detected_at)}
                </span>
              </div>
              <div className="text-sm font-medium text-slate-100">{anomaly.title}</div>
              <div className="mt-0.5 text-[11px] uppercase tracking-wide text-slate-500">
                {TYPE_LABELS[anomaly.type] ?? anomaly.type}
              </div>
            </button>
          );
        })}
      </div>
    </section>
  );
}
