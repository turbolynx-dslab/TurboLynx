"use client";
import { useEffect, useRef, useState } from "react";
import { useInView } from "framer-motion";

interface Props {
  target: number;
  duration?: number;
  decimals?: number;
  suffix?: string;
  prefix?: string;
  separator?: boolean;
}

export default function AnimatedCounter({ target, duration = 2000, decimals = 0, suffix = "", prefix = "", separator = true }: Props) {
  const ref = useRef<HTMLSpanElement>(null);
  const isInView = useInView(ref, { once: true });
  const [value, setValue] = useState(0);

  useEffect(() => {
    if (!isInView) return;
    const start = performance.now();
    const raf = (now: number) => {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);
      // ease out cubic
      const eased = 1 - Math.pow(1 - progress, 3);
      const current = eased * target;
      setValue(current);
      if (progress < 1) requestAnimationFrame(raf);
    };
    requestAnimationFrame(raf);
  }, [isInView, target, duration]);

  const formatted = separator
    ? value.toFixed(decimals).replace(/\B(?=(\d{3})+(?!\d))/g, ",")
    : value.toFixed(decimals);

  return (
    <span ref={ref}>
      {prefix}{formatted}{suffix}
    </span>
  );
}
