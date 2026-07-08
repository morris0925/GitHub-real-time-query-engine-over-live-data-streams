"use client";

/**
 * Active-query mode (§1): free-text question → POST /query → same
 * RAG + Haiku pipeline as anomaly diagnosis, rendered in the detail panel.
 */

import { FormEvent, useState } from "react";

export default function QueryBox({
  onSubmit,
  busy,
}: {
  onSubmit: (question: string) => void;
  busy: boolean;
}) {
  const [question, setQuestion] = useState("");

  const handleSubmit = (event: FormEvent) => {
    event.preventDefault();
    const trimmed = question.trim();
    if (!trimmed || busy) return;
    onSubmit(trimmed);
  };

  return (
    <form
      onSubmit={handleSubmit}
      className="flex gap-2 rounded-lg border border-slate-800 bg-slate-900/60 p-2"
      aria-label="Ask a question"
    >
      <input
        value={question}
        onChange={(event) => setQuestion(event.target.value)}
        placeholder='Ask about the pipeline, e.g. "why did the CI fail rate spike yesterday afternoon?"'
        className="flex-1 rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-200 placeholder:text-slate-600 focus:border-slate-500 focus:outline-none"
        disabled={busy}
      />
      <button
        type="submit"
        disabled={busy || !question.trim()}
        className="rounded-md border border-violet-500/50 bg-violet-500/15 px-4 py-2 text-sm font-medium text-violet-200 transition-colors hover:bg-violet-500/25 disabled:cursor-not-allowed disabled:opacity-40"
      >
        {busy ? "Diagnosing…" : "Ask ✦"}
      </button>
    </form>
  );
}
