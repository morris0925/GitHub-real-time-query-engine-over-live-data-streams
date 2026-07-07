# StreamLens AI Layer — User-Facing Interface Design Proposal

**Context:** Solo portfolio demo. Frontend stack: Next.js (reusing experience from the Down project). Timeline: not yet fixed — this proposal is tiered so you can pick a scope after seeing the trade-offs.

---

## 1. Core Interaction Model

**Both modes, but simulated within a single Next.js app — no Slack/Teams bot for the demo.**

| Mode | Use case | Interface |
|---|---|---|
| Passive notification | Anomaly fires while you're not looking → engineer needs a low-friction heads-up | Toast + new card appended to an "Incident Feed" list, driven by polling `/api/anomalies` every 5–10s |
| Active query | Engineer has a hunch, wants to ask "why did CI fail rate spike yesterday around 3pm?" | Search/chat input box on the same page, hits `/api/query`, runs RAG + Haiku live |

Rationale for skipping a real Slack bot: for a solo demo, a fake Slack integration adds setup risk (webhook, workspace, OAuth) without adding signal — an interviewer cares about the RAG + diagnosis quality, not whether Slack is wired up. Note it explicitly as "Phase 2 — production would push to Slack/Teams" in devlog.md. That's a stronger interview story than a half-working bot.

---

## 2. Information Architecture (per diagnosis result)

Ordered top → bottom by visual priority:

1. **Header row** — anomaly title, severity badge (color-coded: red/orange/yellow), timestamp, anomaly type (CI failure spike / merge time anomaly / commit drought). Glanceable in under 2 seconds.
2. **AI trust banner** — persistent, sits directly under the header, before any generated text is read. See §4.
3. **Diagnosis summary** — 2–4 sentence LLM-generated explanation. This is the payload the engineer came for.
4. **Historical outcome estimate** — data-backed impact estimate (revert rate, average time-to-resolve for similar past cases). Not LLM-generated — see §3. Sits next to the summary as a factual counterweight to it.
5. **Suggested actions** — short checklist, imperative voice ("Check recent deploy to `payments-service`", "Compare with incident #142").
6. **Similar historical cases** — collapsible list; each entry shows similarity band, source link (issue/PR), label metadata (e.g. `severity:high`, `reverted`), and a one-line snippet of what happened. Supporting evidence, not the headline.
7. **Metric context** — small chart of the underlying signal (e.g., CI failure rate over time) so the engineer can eyeball the anomaly itself, independent of the LLM's interpretation.
8. **Raw retrieved evidence** — collapsed by default. Exact chunks fed into the LLM. For the skeptical engineer who wants to audit the RAG grounding.

Reasoning: severity + summary answer "how bad, what happened" fastest; the historical outcome estimate sits immediately beside the summary because it's the one piece of "how bad" that's actually grounded in fact rather than inference; actions come next as the call-to-action; raw evidence is opt-in because most people won't need it, but it must be reachable for trust.

---

## 3. Diagnosing Harm to the Overall System/Project

You only have GitHub Events data (PRs, commits, CI status, issues) — no APM, no production error rates, no real user-impact telemetry. That caps how far "harm diagnosis" can honestly go. Three tiers, in order of how much they cost and how far they reach:

**Tier 1 — development-process proxy (already covered by the anomaly detector).**
CI failure rate, merge time, commit frequency. Tells you the dev process looks unstable. Does not tell you production is actually hurting.

**Tier 2 — historical-outcome mining (recommended addition, no new data source needed).**
Your closed issues/PRs already carry structured metadata beyond their text: labels (`severity:high`, `P0`, `hotfix`), whether a later PR titled "Revert ..." points back at the change, and how long the issue took to close. Mine these three signals per historical case and attach them to the RAG retrieval results:

- **Revert rate** — of similar past cases, what fraction were eventually reverted
- **Time-to-resolve** — average close time for similar past cases
- **Blast radius (fan-out)** — how many files/modules the current change touches, computed directly from existing schema, cross-referenced against whether large-fan-out changes historically reverted more often

None of this needs the LLM — it's DuckDB aggregation over metadata you already have access to once the knowledge base ingests labels and revert-linkage, not just embedded text. It gives a real, defensible answer to "how much did situations like this actually hurt, historically" and sits next to the LLM's narrative summary as a factual check on it (see §2, item 4).

**Tier 3 — real production impact (out of scope, requires new data source).**
Actual user-facing severity requires ingesting something outside GitHub — APM (Datadog/New Relic), error tracking (Sentry), or an incident tool (PagerDuty). This is a genuine architecture expansion beyond "Kafka ingests GitHub Events," not something the AI layer can produce from existing data. Flag as an explicit future direction, not part of this MVP.

**Project-level signal indicator.**
A single status light aggregating all unresolved anomalies, shown once at the top of the dashboard — distinct from the per-anomaly severity badge in §2. Framed honestly as a **"Dev Pipeline Signal"**, not "project health," since it's built entirely from Tier 1 data. Implementation notes:

- Rendered as a status bar, not a badge, and in a color distinct from both the severity badges and the AI-content violet, so three different signals never look alike
- Broken into its three underlying components (CI stability / PR velocity / commit cadence) shown side by side rather than collapsed into one number — this lets the engineer tell "CI is actually failing" apart from "everyone's on vacation, commit frequency naturally dropped," which the raw detector can't distinguish on its own
- Carries a persistent caption: "Based on CI/PR/commit signals — not a live production health check"

---

## 4. Trust Labeling for AI-Generated Content

- **Color:** reserve red/orange/yellow for severity. Use a distinct accent (violet/purple) exclusively for anything LLM-generated, so severity and "this is AI" never compete for the same visual channel.
- **Icon + label on every generated block** (not just once at the top of the page) — a small sparkle icon + "AI-generated · verify before acting" caption on the summary, actions, and similar-cases blocks individually. Engineers skim under pressure and will miss a single page-level disclaimer.
- **Confidence as a qualitative band**, not a fake-precision percentage: "High / Medium / Low similarity match" rather than "87% confidence" — a number implies false precision for a similarity search over historical text.
- **Hedged language enforced at the prompt level**, not just the UI: the Haiku system prompt should require phrasing like "likely related to…" / "similar pattern seen in…" rather than "this is caused by…". This is the single highest-leverage trust control — UI labels help, but the generated text itself must not overclaim.
- **One-time footer disclaimer** per page as a backstop: "Diagnoses are generated by an LLM based on historical pattern matching. They are suggestions, not conclusions — validate against current logs/metrics before acting."
- The Tier 2 historical outcome estimate (§3) is explicitly *not* under this labeling — it's computed, not generated, and should look visually distinct from the AI content it sits beside.

---

## 5. Technical Form

**Next.js web dashboard as the single interface**, given your stated preference and existing Next.js experience.

Architecture: keep the existing Python core (Kafka, DuckDB, PyArrow) untouched. Add a thin FastAPI service that exposes:

- `GET /anomalies` — list detected anomalies (from the existing rule-based detector)
- `GET /diagnose/:id` — fetch or trigger a cached RAG + Haiku diagnosis for one anomaly, including the Tier 2 historical-outcome estimate
- `POST /query` — ad hoc natural-language question → RAG + Haiku, for the active-query mode

Next.js talks to this FastAPI service over HTTP; it does not reimplement any data logic. This keeps "pipeline logic, storage logic, dashboard logic" separated per your existing project rule — the AI layer becomes a peer service, not a rewrite of the Python core.

Explicitly not building for the demo: CLI tool (the Rich terminal dashboard stays as-is and is a separate concern), Slack/Teams bot (see §1).

---

## 6. MVP Scope

### 1-week minimal demo
- FastAPI service with the 3 endpoints above, wrapping existing DuckDB reader + new DuckDB VSS retrieval + one Haiku call
- Knowledge base enrichment: ingest issue/PR labels and revert-linkage alongside embedded text (needed for §3 Tier 2)
- One Next.js page: incident feed (left) + diagnosis detail panel (right) following the §2 layout, including the historical outcome estimate
- Dev Pipeline Signal bar at the top of the page (§3), broken into its three components
- A "trigger demo anomaly" button to seed anomalies on demand — live GitHub events may not reliably produce one during a live demo
- Active-query text box calling `/api/query`
- Polling instead of real push (5–10s interval is indistinguishable from "real-time" in a demo)
- No auth, no persisted user feedback

### 2–4 week extension (explicitly out of the 1-week scope)
- Metric chart in the diagnosis panel (§2 item 7)
- 👍/👎 feedback buttons on diagnoses (sets up future eval layer, without building it)
- Real Slack webhook for push notifications
- Visual polish: loading skeletons, animations, dark theme matching the terminal dashboard's aesthetic

### Out of scope regardless of timeline
Auto-ticket creation, multi-turn agent conversation, evaluation framework, multi-user/auth, real Slack/Teams bot beyond a stretch-goal mention, Tier 3 real production-impact diagnosis (needs a new data source — APM/incident tool). These match the MVP boundary already set for the AI layer itself.

---

## Open questions before implementation

1. Anomaly seeding: is a manual "trigger demo anomaly" button acceptable, or do you want the demo to rely on genuinely live GitHub event anomalies?
2. Should the FastAPI service live in this repo (`src/api/`) or as a sibling service, given the "no web server container" rule currently in CLAUDE.md — that rule will need an explicit amendment once this layer is approved.
3. Do your existing closed issues/PRs actually carry severity labels or revert-linkage in practice, or would Tier 2 need you to retroactively label a sample of historical cases to have any signal to mine?
