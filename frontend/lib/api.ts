/**
 * lib/api.ts — Typed client for the StreamLens AI diagnostic API.
 *
 * The frontend never reshapes or recomputes data (project rule): these types
 * mirror src/api/schemas.py one-to-one, and every function is a thin fetch.
 */

export const API_BASE: string =
  process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8000";

export type Severity = "high" | "medium" | "low";
export type Band = "high" | "medium" | "low";
export type SignalStatus = "ok" | "warn" | "alert" | "unknown";

export interface Anomaly {
  anomaly_id: string;
  type: string;
  title: string;
  severity: Severity;
  description: string | null;
  metric: Record<string, number | null>;
  repo: string | null;
  detected_at: string;
  is_demo: boolean;
}

export interface SignalComponent {
  status: SignalStatus;
  [metric: string]: number | string | null;
}

export interface PipelineSignal {
  ci_stability: SignalComponent;
  pr_velocity: SignalComponent;
  commit_cadence: SignalComponent;
  caption: string;
}

export interface SimilarCase {
  case_id: string;
  kind: "issue" | "pr";
  title: string;
  url: string | null;
  labels: string[];
  similarity_band: Band;
  was_reverted: boolean;
  time_to_resolve_hours: number | null;
  snippet: string | null;
}

export interface OutcomeEstimate {
  sample_size: number;
  pr_count: number;
  revert_rate: number | null;
  avg_time_to_resolve_hours: number | null;
  severity_labels: string[];
}

export interface GeneratedContent {
  summary: string;
  suggested_actions: string[];
  confidence: Band;
  ai_notice: string;
}

export interface DiagnosisMeta {
  llm_provider: string;
  llm_model: string | null;
  embedding_provider: string;
  generated_at: string;
  disclaimer: string;
}

export interface Diagnosis {
  anomaly: Anomaly;
  generated: GeneratedContent;
  outcome_estimate: OutcomeEstimate;
  similar_cases: SimilarCase[];
  raw_evidence: string[];
  meta: DiagnosisMeta;
}

export interface QueryResponse {
  question: string;
  generated: GeneratedContent;
  outcome_estimate: OutcomeEstimate;
  similar_cases: SimilarCase[];
  raw_evidence: string[];
  meta: DiagnosisMeta;
}

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) throw new Error(`GET ${path} → ${response.status}`);
  return response.json() as Promise<T>;
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`POST ${path} → ${response.status}`);
  return response.json() as Promise<T>;
}

export const fetchAnomalies = () => get<Anomaly[]>("/anomalies");
export const fetchSignal = () => get<PipelineSignal>("/signal");
export const fetchDiagnosis = (anomalyId: string) =>
  get<Diagnosis>(`/diagnose/${encodeURIComponent(anomalyId)}`);
export const postQuery = (question: string) =>
  post<QueryResponse>("/query", { question });
// Capture the repo's REAL current CI state as an anomaly. No synthetic
// fallback: if there is no CI data yet the API returns 503, surfaced to the
// user rather than papered over with invented numbers.
export const snapshotCiState = () => post<Anomaly>("/snapshot", {});
