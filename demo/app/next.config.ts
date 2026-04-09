import type { NextConfig } from "next";

const basePath = "/TurboLynx/demo";

const nextConfig: NextConfig = {
  devIndicators: false,
  output: "export",
  basePath,
  assetPrefix: basePath,
  turbopack: {
    root: ".",
  },
  env: {
    NEXT_PUBLIC_BASE_PATH: basePath,
  },
};

export default nextConfig;
