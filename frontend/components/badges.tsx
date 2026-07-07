/**
 * Shared visual primitives, enforcing the §4 color separation:
 * severity = red/orange/yellow fills; AI content = violet only.
 */

import { Band, Severity } from "@/lib/api";

const SEVERITY_STYLES: Record<Severity, string> = {
  high: "bg-red-500/20 text-red-300 border-red-500/50",
  medium: "bg-orange-500/20 text-orange-300 border-orange-500/50",
  low: "bg-yellow-500/20 text-yellow-300 border-yellow-500/50",
};

export function SeverityBadge({ severity }: { severity: Severity }) {
  return (
    <span
      className={`rounded border px-1.5 py-0.5 text-[11px] font-semibold uppercase tracking-wide ${
        SEVERITY_STYLES[severity] ?? SEVERITY_STYLES.low
      }`}
    >
      {severity}
    </span>
  );
}

/** Qualitative similarity/confidence band — deliberately never a percentage. */
export function BandBadge({ band, prefix }: { band: Band; prefix?: string }) {
  return (
    <span className="rounded border border-slate-600 bg-slate-800 px-1.5 py-0.5 text-[11px] text-slate-300">
      {prefix ? `${prefix}: ` : ""}
      {band} match
    </span>
  );
}

/**
 * The per-block AI label (§4): sparkle + caption on EVERY generated block,
 * not once per page — engineers skim under pressure.
 */
export function AiBlockLabel({ notice }: { notice: string }) {
  return (
    <div
      className="mb-2 flex items-center gap-1.5 text-[11px] font-medium"
      style={{ color: "var(--ai-accent)" }}
    >
      <span aria-hidden>✦</span>
      {notice}
    </div>
  );
}

/** Wrapper giving LLM-generated blocks their exclusive violet accent. */
export function AiBlock({
  notice,
  children,
}: {
  notice: string;
  children: React.ReactNode;
}) {
  return (
    <div
      className="rounded-md border border-violet-500/40 bg-violet-500/5 p-3"
      style={{ borderLeftColor: "var(--ai-accent)", borderLeftWidth: 3 }}
    >
      <AiBlockLabel notice={notice} />
      {children}
    </div>
  );
}
