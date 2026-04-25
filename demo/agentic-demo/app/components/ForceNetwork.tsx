"use client";
import { useEffect, useMemo, useRef, useState } from "react";

type NetNode = {
  i: number;
  name: string;
  role: "director" | "collaborator";
  c: number;
  pr: number;
};
type NetEdge = { s: number; t: number; w: number };
type NetData = {
  nodes: NetNode[];
  edges: NetEdge[];
  stats: { n_directors: number; n_collaborators: number; n_edges: number; n_communities: number };
};

/** Palette used across community views. First 12 entries are the visually
 *  distinct ones; small communities past that cycle through the list.
 *  Tuned for a white canvas — slightly darker/saturated than pure dark-mode
 *  variants so nodes don't wash out. */
const COMMUNITY_COLORS = [
  "#6d28d9", "#0891b2", "#d97706", "#15803d", "#b91c1c",
  "#9333ea", "#0d9488", "#b45309", "#be185d", "#4338ca",
  "#ea580c", "#7c3aed",
];
const EDGE_COLOR = "#d4d4d8";
const EDGE_COLOR_SAME_COMMUNITY_ALPHA = "66"; // appended to community hex
const NODE_BORDER = "#18181b";
const NODE_DIRECTOR_FILL = "#e84545";
const NODE_COLLABORATOR_FILL = "#a1a1aa";
const LABEL_COLOR = "#18181b";

/** Simulation constants — tuned for 500-1000 nodes on a 2020-class laptop. */
const REPULSION = 1200;
const SPRING_K = 0.025;
const SPRING_LEN = 40;
const CENTER_PULL = 0.008;
const DAMPING = 0.86;
const MAX_VEL = 6;

interface Props {
  mode: "plain" | "community" | "centrality";
  width: number;
  height: number;
}

export default function ForceNetwork({ mode, width, height }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [data, setData] = useState<NetData | null>(null);
  const [hover, setHover] = useState<{ x: number; y: number; label: string } | null>(null);
  // View transform — pan (tx, ty) + zoom (k). Default zoomed-out so the
  // whole network fits with margin. Drag to pan, wheel to zoom.
  const view = useRef<{ k: number; tx: number; ty: number }>({ k: 0.55, tx: 0, ty: 0 });
  const [, bump] = useState(0); // force re-render on view changes
  const dragRef = useRef<{ startX: number; startY: number; startTx: number; startTy: number } | null>(null);

  // Single JSON fetch — reused across mode switches.
  useEffect(() => {
    const base = process.env.NEXT_PUBLIC_BASE_PATH || "";
    fetch(`${base}/network.json`)
      .then((r) => r.json())
      .then((d: NetData) => setData(d))
      .catch(() => setData(null));
  }, []);

  // ---- Simulation state lives in refs so re-renders don't reset layout --
  const state = useRef<{
    pos: Float32Array;
    vel: Float32Array;
    frozen: boolean;
    edges: NetEdge[] | null;
  }>({ pos: new Float32Array(0), vel: new Float32Array(0), frozen: false, edges: null });

  useEffect(() => {
    if (!data) return;
    const n = data.nodes.length;
    const pos = new Float32Array(n * 2);
    const vel = new Float32Array(n * 2);
    // Deterministic seed layout — concentric by pagerank so high-PR nodes
    // start near the center and low-PR nodes on the rim. Works well both
    // for visual and for faster relaxation.
    for (let i = 0; i < n; i++) {
      const a = (i / n) * Math.PI * 2;
      const r = 80 + (1 - data.nodes[i].pr) * 200;
      pos[i * 2] = Math.cos(a) * r;
      pos[i * 2 + 1] = Math.sin(a) * r;
    }
    state.current = { pos, vel, frozen: false, edges: data.edges };
  }, [data]);

  // Main render/simulation loop
  useEffect(() => {
    if (!data || !canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    let raf = 0;
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    let ticks = 0;
    const cx = width / 2;
    const cy = height / 2;

    const tick = () => {
      const { pos, vel, edges } = state.current;
      if (!edges) return;
      const n = data.nodes.length;

      if (!state.current.frozen) {
        // Repulsion — O(n²), fine for <1.5k nodes at 60fps. Switch to
        // Barnes-Hut if the dataset grows past that.
        for (let i = 0; i < n; i++) {
          let fx = 0;
          let fy = 0;
          const ix = pos[i * 2];
          const iy = pos[i * 2 + 1];
          for (let j = 0; j < n; j++) {
            if (i === j) continue;
            const dx = ix - pos[j * 2];
            const dy = iy - pos[j * 2 + 1];
            const d2 = dx * dx + dy * dy + 0.01;
            const inv = REPULSION / d2;
            fx += dx * inv;
            fy += dy * inv;
          }
          // Centering pull
          fx -= ix * CENTER_PULL;
          fy -= iy * CENTER_PULL;

          vel[i * 2] = (vel[i * 2] + fx * 0.016) * DAMPING;
          vel[i * 2 + 1] = (vel[i * 2 + 1] + fy * 0.016) * DAMPING;
        }
        // Spring attraction on edges
        for (let e = 0; e < edges.length; e++) {
          const { s, t, w } = edges[e];
          const dx = pos[t * 2] - pos[s * 2];
          const dy = pos[t * 2 + 1] - pos[s * 2 + 1];
          const dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
          const stiffness = SPRING_K * Math.log(1 + w);
          const delta = (dist - SPRING_LEN) * stiffness;
          const ux = (dx / dist) * delta;
          const uy = (dy / dist) * delta;
          vel[s * 2] += ux;
          vel[s * 2 + 1] += uy;
          vel[t * 2] -= ux;
          vel[t * 2 + 1] -= uy;
        }
        // Integrate + clamp velocities
        for (let i = 0; i < n; i++) {
          if (vel[i * 2] > MAX_VEL) vel[i * 2] = MAX_VEL;
          else if (vel[i * 2] < -MAX_VEL) vel[i * 2] = -MAX_VEL;
          if (vel[i * 2 + 1] > MAX_VEL) vel[i * 2 + 1] = MAX_VEL;
          else if (vel[i * 2 + 1] < -MAX_VEL) vel[i * 2 + 1] = -MAX_VEL;
          pos[i * 2] += vel[i * 2];
          pos[i * 2 + 1] += vel[i * 2 + 1];
        }
        ticks++;
        // Freeze after an energy-budgeted warm-up to save CPU when hovering
        if (ticks > 220) state.current.frozen = true;
      }

      // ---- draw ----
      ctx.clearRect(0, 0, width, height);
      ctx.save();
      // Center + apply current view transform. All drawing below is in
      // "world" coordinates; the transform handles pan + zoom.
      ctx.translate(cx + view.current.tx, cy + view.current.ty);
      ctx.scale(view.current.k, view.current.k);

      // Edges
      ctx.lineWidth = 0.7;
      for (const e of edges) {
        const sx = pos[e.s * 2];
        const sy = pos[e.s * 2 + 1];
        const tx = pos[e.t * 2];
        const ty = pos[e.t * 2 + 1];
        if (mode === "community") {
          const ca = data.nodes[e.s].c;
          const cb = data.nodes[e.t].c;
          ctx.strokeStyle = ca === cb
            ? COMMUNITY_COLORS[ca % COMMUNITY_COLORS.length] + EDGE_COLOR_SAME_COMMUNITY_ALPHA
            : EDGE_COLOR;
        } else {
          ctx.strokeStyle = EDGE_COLOR;
        }
        ctx.beginPath();
        ctx.moveTo(sx, sy);
        ctx.lineTo(tx, ty);
        ctx.stroke();
      }

      // Nodes
      for (let i = 0; i < n; i++) {
        const node = data.nodes[i];
        const x = pos[i * 2];
        const y = pos[i * 2 + 1];
        let r = node.role === "director" ? 4.5 : 3;
        let color = node.role === "director" ? NODE_DIRECTOR_FILL : NODE_COLLABORATOR_FILL;
        if (mode === "community") {
          color = COMMUNITY_COLORS[node.c % COMMUNITY_COLORS.length];
        } else if (mode === "centrality") {
          r = 2 + node.pr * 22;
          color = COMMUNITY_COLORS[node.c % COMMUNITY_COLORS.length];
        }
        ctx.beginPath();
        ctx.fillStyle = color;
        ctx.arc(x, y, r, 0, Math.PI * 2);
        ctx.fill();
        // Hair-thin dark outline on every node for crisp edges on white bg
        ctx.strokeStyle = NODE_BORDER;
        ctx.lineWidth = node.role === "director" || node.pr > 0.6 ? 1.2 : 0.35;
        ctx.stroke();
      }

      // Labels for notable nodes only — high-PR or director.
      ctx.font = '11px "Inter", -apple-system, sans-serif';
      ctx.fillStyle = LABEL_COLOR;
      for (let i = 0; i < n; i++) {
        const node = data.nodes[i];
        if (node.pr < 0.45 && node.role !== "director") continue;
        const x = pos[i * 2];
        const y = pos[i * 2 + 1];
        // Subtle white halo for readability when labels land on top of edges
        ctx.strokeStyle = "#ffffffcc";
        ctx.lineWidth = 3;
        ctx.strokeText(node.name, x + 8, y - 4);
        ctx.fillText(node.name, x + 8, y - 4);
      }

      ctx.restore();
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [data, mode, width, height]);

  const screenToWorld = (sx: number, sy: number) => {
    // Inverse of the ctx transform in tick(): translate + scale.
    const { k, tx, ty } = view.current;
    return {
      x: (sx - width / 2 - tx) / k,
      y: (sy - height / 2 - ty) / k,
    };
  };

  const onMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!data || !canvasRef.current) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const sx = e.clientX - rect.left;
    const sy = e.clientY - rect.top;

    // Panning takes priority over hover
    if (dragRef.current) {
      view.current.tx = dragRef.current.startTx + (sx - dragRef.current.startX);
      view.current.ty = dragRef.current.startTy + (sy - dragRef.current.startY);
      state.current.frozen = true;  // freeze physics during interaction
      bump((x) => x + 1);
      return;
    }

    const { x: wx, y: wy } = screenToWorld(sx, sy);
    const { pos } = state.current;
    let best = -1;
    // Hit radius in world space — scaled up when zoomed out so tiny nodes
    // are still easy to point at.
    const hitR = 12 / Math.max(0.3, view.current.k);
    let bestD = hitR * hitR;
    for (let i = 0; i < data.nodes.length; i++) {
      const dx = wx - pos[i * 2];
      const dy = wy - pos[i * 2 + 1];
      const d = dx * dx + dy * dy;
      if (d < bestD) {
        bestD = d;
        best = i;
      }
    }
    if (best >= 0) {
      setHover({ x: e.clientX, y: e.clientY, label: data.nodes[best].name });
    } else {
      setHover(null);
    }
  };

  const onWheel = (e: React.WheelEvent<HTMLCanvasElement>) => {
    if (!canvasRef.current) return;
    e.preventDefault();
    const rect = canvasRef.current.getBoundingClientRect();
    const sx = e.clientX - rect.left;
    const sy = e.clientY - rect.top;
    // Anchor zoom under cursor
    const world = screenToWorld(sx, sy);
    const factor = Math.exp(-e.deltaY * 0.0015);
    const k = Math.max(0.15, Math.min(3.5, view.current.k * factor));
    view.current.k = k;
    // re-anchor: translate so `world` stays under cursor
    view.current.tx = sx - width / 2 - world.x * k;
    view.current.ty = sy - height / 2 - world.y * k;
    bump((x) => x + 1);
  };

  const onDown = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!canvasRef.current) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const sx = e.clientX - rect.left;
    const sy = e.clientY - rect.top;
    dragRef.current = {
      startX: sx,
      startY: sy,
      startTx: view.current.tx,
      startTy: view.current.ty,
    };
  };
  const onUp = () => {
    dragRef.current = null;
  };

  const zoom = (factor: number) => {
    const k = Math.max(0.15, Math.min(3.5, view.current.k * factor));
    view.current.k = k;
    bump((x) => x + 1);
  };
  const reset = () => {
    view.current = { k: 0.55, tx: 0, ty: 0 };
    bump((x) => x + 1);
  };

  const roleSummary = useMemo(() => {
    if (!data) return null;
    return data.stats;
  }, [data]);

  return (
    <div className="absolute inset-0">
      <canvas
        ref={canvasRef}
        onMouseMove={onMove}
        onMouseDown={onDown}
        onMouseUp={onUp}
        onMouseLeave={() => { setHover(null); dragRef.current = null; }}
        onWheel={onWheel}
        className="w-full h-full block"
        style={{ cursor: dragRef.current ? "grabbing" : "grab" }}
      />
      {/* zoom controls */}
      <div className="viz-zoom">
        <button className="viz-zoom__btn" onClick={() => zoom(1.25)} title="Zoom in">+</button>
        <button className="viz-zoom__btn" onClick={() => zoom(0.8)}  title="Zoom out">−</button>
        <button className="viz-zoom__btn" onClick={reset}             title="Reset view">⟳</button>
      </div>
      {/* overlay legend */}
      {roleSummary && (
        <div className="absolute top-3 left-3 bg-white/90 border border-[var(--border)] rounded-lg px-3 py-2 backdrop-blur-sm text-[11px] text-[var(--text-secondary)] mono leading-[1.5] shadow-sm">
          <div>nodes <span className="text-[var(--text-primary)]">{data!.nodes.length.toLocaleString()}</span></div>
          <div>edges <span className="text-[var(--text-primary)]">{roleSummary.n_edges.toLocaleString()}</span></div>
          {mode !== "plain" && (
            <div>communities <span className="text-[var(--text-primary)]">{roleSummary.n_communities}</span></div>
          )}
        </div>
      )}
      {hover && (
        <div
          className="pointer-events-none fixed z-50 px-2 py-1 rounded-md bg-white border border-[var(--border)] text-xs text-[var(--text-primary)] whitespace-nowrap shadow-md"
          style={{ left: hover.x + 12, top: hover.y + 12 }}
        >
          {hover.label}
        </div>
      )}
    </div>
  );
}
