import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "StreamLens — AI Diagnostics",
  description:
    "AI diagnostic layer over the StreamLens GitHub-events pipeline. Diagnoses are LLM suggestions, not conclusions.",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
