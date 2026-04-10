import type { NextConfig } from "next";

const basePath = "/demo";

const nextConfig: NextConfig = {
  devIndicators: false,
  output: "export",
  trailingSlash: true,
  basePath,
  assetPrefix: basePath,
  turbopack: {
    root: ".",
  },
  env: {
    NEXT_PUBLIC_BASE_PATH: basePath,
    NEXT_PUBLIC_API_URL: "https://turbolynx.duckdns.org:8080",
  },
};

export default nextConfig;
