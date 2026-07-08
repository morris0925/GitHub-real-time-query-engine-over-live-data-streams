"use client";

/**
 * Diagnosis detail panel (right column) — §2 information architecture,
 * strictly top-to-bottom:
 *
 *   1. Header row (title, severity badge, timestamp, anomaly type)
 *   2. AI trust banner — before any generated text is read
 *   3. LLM summary                      (violet AI block)
 *   4. Historical outcome estimate      (computed — deliberately NOT violet)
 *   5. Suggested actions                (violet AI block)
 *   6. Similar historical cases         (violet label; collapsible entries)
 *   7. Raw retrieved evidence           (collapsed by default)
 */

import { Diagnosis, OutcomeEstimate, QueryResponse, SimilarCase } from "@/lib/api";
import { AiBlock, BandBadge, SeverityBadge } from "@/components/badges";

/** The panel renders either an anomaly diagnosis or a free-text query answer. */
export type PanelResult = Diagnosis | QueryResponse;

function isDiagnosis(result: PanelResult): result is Diagnosis {
  return "anomaly" in result;
}

const TYPE_LABELS: Record<string, string> = {
  ci_failure_spike: "CI failure spike",
  merge_time_anomaly: "Merge time anomaly",
  commit_drought: "Commit drought",
};

function TrustBanner({ confidence, llmProvider }: { confidence: string; llmProvider: string }) {
  return (
    <div
      className="flex flex-wrap items-center gap-x-3 gap-y-1 rounded-md border border-violet-500/40 bg-violet-500/10 px-3 py-2 text-xs text-violet-200"
      style={{ borderLeftColor: "var(--ai-accent)", borderLeftWidth: 3 }}
      role="note"
    >
      <span aria-hidden>✦</span>
      <span className="font-medium">
        The sections marked ✦ below are AI-generated from historical pattern
        matching — verify before acting.
      </span>
      <span className="ml-auto flex items-center gap-2">
        <BandBadge band={confidence as SimilarCase["similarity_band"]} prefix="confidence" />
        {llmProvider === "stub" && (
          <span className="rounded border border-amber-500/50 bg-amber-500/10 px-1.5 py-0.5 text-[11px] text-amber-300">
            no LLM key — placeholder output
          </span>
        )}
      </span>
    </div>
  );
}

function OutcomeBlock({ outcome }: { outcome: OutcomeEstimate }) {
  const insufficient = outcome.sample_size === 0;
  return (
    <div className="rounded-md border border-slate-700 bg-slate-800/60 p-3">
      <div className="mb-2 text-[11px] font-medium uppercase tracking-wide text-slate-400">
        Historical outcome estimate · computed from {outcome.sample_size} similar
        cases — not AI-generated
      </div>
      {insufficient ? (
        <p className="text-sm text-slate-400">
          Insufficient historical data for an outcome estimate.
        </p>
      ) : (
        <div className="grid grid-cols-3 gap-3 text-sm">
          <div>
            <div className="text-lg font-semibold text-slate-100">
              {outcome.revert_rate === null
                ? "—"
                : `${Math.round(outcome.revert_rate * 100)}%`}
            </div>
            <div className="text-[11px] text-slate-400">
              of {outcome.pr_count} similar PRs were reverted
            </div>
          </div>
          <div>
            <div className="text-lg font-semibold text-slate-100">
              {outcome.avg_time_to_resolve_hours === null
                ? "—"
                : `${outcome.avg_time_to_resolve_hours.toFixed(1)}h`}
            </div>
            <div className="text-[11px] text-slate-400">avg time to resolve</div>
          </div>
          <div>
            <div className="flex flex-wrap gap-1">
              {outcome.severity_labels.length === 0 ? (
                <span className="text-lg font-semibold text-slate-100">—</span>
              ) : (
                outcome.severity_labels.map((label) => (
                  <span
                    key={label}
                    className="rounded bg-slate-700 px-1.5 py-0.5 text-[11px] text-slate-200"
                  >
                    {label}
                  </span>
                ))
              )}
            </div>
            <div className="mt-1 text-[11px] text-slate-400">
              severity labels seen historically
            </div>
          </div>
        </div>
      )}
      {!insufficient && outcome.sample_size < 3 && (
        <p className="mt-2 text-[11px] text-amber-400">
          Small sample — treat these numbers as weak evidence.
        </p>
      )}
    </div>
  );
}

function SimilarCases({ cases, notice }: { cases: SimilarCase[]; notice: string }) {
  return (
    <AiBlock notice={`Similar historical cases · retrieval-ranked · ${notice}`}>
      {cases.length === 0 ? (
        <p className="text-sm text-slate-400">No similar cases retrieved.</p>
      ) : (
        <ul className="space-y-2">
          {cases.map((c) => (
            <li key={c.case_id}>
              <details className="group rounded border border-slate-700/60 bg-slate-900/50 px-3 py-2">
                <summary className="flex cursor-pointer list-none flex-wrap items-center gap-2 text-sm">
                  <span className="text-[11px] uppercase text-slate-500">{c.kind}</span>
                  <span className="font-medium text-slate-200">{c.title}</span>
                  <span className="ml-auto flex items-center gap-1.5">
                    <BandBadge band={c.similarity_band} />
                    {c.was_reverted && (
                      <span className="rounded border border-red-500/50 bg-red-500/10 px-1.5 py-0.5 text-[11px] text-red-300">
                        reverted
                      </span>
                    )}
                  </span>
                </summary>
                <div className="mt-2 space-y-1.5 text-xs text-slate-400">
                  {c.labels.length > 0 && (
                    <div className="flex flex-wrap gap-1">
                      {c.labels.map((label) => (
                        <span key={label} className="rounded bg-slate-800 px-1.5 py-0.5">
                          {label}
                        </span>
                      ))}
                    </div>
                  )}
                  {c.snippet && <p className="text-slate-300">{c.snippet}</p>}
                  <div className="flex items-center gap-3">
                    {c.time_to_resolve_hours !== null && (
                      <span>resolved in {c.time_to_resolve_hours.toFixed(1)}h</span>
                    )}
                    {c.url && (
                      <a
                        href={c.url}
                        target="_blank"
                        rel="noreferrer"
                        className="text-sky-400 hover:underline"
                      >
                        open on GitHub ↗
                      </a>
                    )}
                  </div>
                </div>
              </details>
            </li>
          ))}
        </ul>
      )}
    </AiBlock>
  );
}

export default function DiagnosisPanel({
  result,
  loading,
  error,
}: {
  result: PanelResult | null;
  loading: boolean;
  error: string | null;
}) {
  const diagnosis = result;
  return (
    <section
      aria-label="Diagnosis detail"
      className="flex min-h-0 flex-col overflow-y-auto rounded-lg border border-slate-800 bg-slate-900/60 p-4"
    >
      {!diagnosis && (
        <p className="m-auto max-w-sm text-center text-sm text-slate-500">
          {loading
            ? "Running retrieval + diagnosis…"
            : error ??
              "Select an anomaly from the feed to see its diagnosis, or ask a question below."}
        </p>
      )}

      {diagnosis && (
        <div className="space-y-3">
          {/* 1 — header row */}
          {isDiagnosis(diagnosis) ? (
            <>
              <header className="flex flex-wrap items-center gap-2">
                <SeverityBadge severity={diagnosis.anomaly.severity} />
                <h2 className="text-base font-semibold text-slate-100">
                  {diagnosis.anomaly.title}
                </h2>
                <span className="ml-auto text-[11px] text-slate-500">
                  {TYPE_LABELS[diagnosis.anomaly.type] ?? diagnosis.anomaly.type} ·{" "}
                  {new Date(diagnosis.anomaly.detected_at).toLocaleString()}
                </span>
              </header>
              {diagnosis.anomaly.description && (
                <p className="text-sm text-slate-400">{diagnosis.anomaly.description}</p>
              )}
            </>
          ) : (
            <header className="flex flex-wrap items-center gap-2">
              <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                Question
              </span>
              <h2 className="text-base font-semibold text-slate-100">
                {diagnosis.question}
              </h2>
            </header>
          )}

          {/* 2 — AI trust banner, before any generated text */}
          <TrustBanner
            confidence={diagnosis.generated.confidence}
            llmProvider={diagnosis.meta.llm_provider}
          />

          {/* 3 — LLM summary */}
          <AiBlock notice={diagnosis.generated.ai_notice}>
            <p className="text-sm leading-relaxed text-slate-200">
              {diagnosis.generated.summary}
            </p>
          </AiBlock>

          {/* 4 — computed outcome estimate, the factual counterweight */}
          <OutcomeBlock outcome={diagnosis.outcome_estimate} />

          {/* 5 — suggested actions */}
          <AiBlock notice={`Suggested actions · ${diagnosis.generated.ai_notice}`}>
            {diagnosis.generated.suggested_actions.length === 0 ? (
              <p className="text-sm text-slate-400">No actions suggested.</p>
            ) : (
              <ul className="space-y-1.5 text-sm text-slate-200">
                {diagnosis.generated.suggested_actions.map((action) => (
                  <li key={action} className="flex gap-2">
                    <span className="text-slate-500">☐</span>
                    {action}
                  </li>
                ))}
              </ul>
            )}
          </AiBlock>

          {/* 6 — similar historical cases */}
          <SimilarCases
            cases={diagnosis.similar_cases}
            notice={diagnosis.generated.ai_notice}
          />

          {/* 7 — raw evidence, collapsed by default */}
          <details className="rounded-md border border-slate-800 bg-slate-950/60 px-3 py-2">
            <summary className="cursor-pointer text-xs font-medium uppercase tracking-wide text-slate-500">
              Raw retrieved evidence ({diagnosis.raw_evidence.length} chunks — exactly
              what the LLM saw)
            </summary>
            <div className="mt-2 space-y-2">
              {diagnosis.raw_evidence.map((chunk, i) => (
                <pre
                  key={i}
                  className="overflow-x-auto whitespace-pre-wrap rounded bg-slate-900 p-2 text-[11px] text-slate-400"
                >
                  {chunk}
                </pre>
              ))}
            </div>
          </details>

          <p className="text-right text-[10px] text-slate-600">
            {diagnosis.meta.llm_provider}
            {diagnosis.meta.llm_model ? ` · ${diagnosis.meta.llm_model}` : ""} ·
            embeddings: {diagnosis.meta.embedding_provider}
          </p>
        </div>
      )}
    </section>
  );
}
