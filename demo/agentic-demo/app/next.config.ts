import type { NextConfig } from "next";

// When set by CI / static-deploy this page lives at /demo-agentic/ on turbolynx.io.
// Locally (`pnpm dev`) we want the app at the origin root, so NEXT_PUBLIC_BASE_PATH
// is unset and basePath stays empty.
const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";

const nextConfig: NextConfig = {
  devIndicators: false,
  output: process.env.NEXT_EXPORT === "1" ? "export" : undefined,
  trailingSlash: true,
  basePath: basePath || undefined,
  assetPrefix: basePath || undefined,
  turbopack: { root: "." },
  env: {
    NEXT_PUBLIC_BASE_PATH: basePath,
  },
};

export default nextConfig;
