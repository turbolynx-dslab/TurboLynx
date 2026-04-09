import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  devIndicators: false,
  output: "export",
  basePath: "/TurboLynx/demo",
  assetPrefix: "/TurboLynx/demo",
  turbopack: {
    root: ".",
  },
};

export default nextConfig;
