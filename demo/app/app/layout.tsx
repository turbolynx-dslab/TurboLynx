import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "TurboLynx — Schemaless Graph Engine",
  description: "77M nodes, 282,764 unique attribute sets. Every other engine chokes. TurboLynx doesn't. VLDB 2026.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="antialiased">{children}</body>
    </html>
  );
}
