import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "TurboLynx Agentic DB — Cinema Demo",
  description:
    "LLM agent explores a DBpedia cinema graph. TurboLynx serves the data; analysis is generated on-the-fly as Python code.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="antialiased">{children}</body>
    </html>
  );
}
