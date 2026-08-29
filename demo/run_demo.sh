#!/usr/bin/env bash
# =============================================================================
# THE one command for the VLDB booth feature-ladder demo.
#
#   ./run_demo.sh                 # generate/load if needed, then serve
#   ./run_demo.sh --regen         # force a full data rebuild first
#   ./run_demo.sh --port 8600     # serve somewhere else
#   ./run_demo.sh --no-fonts      # skip the offline font fetch (needs internet
#                                 # at display time instead)
#
# It: checks the workspace (offers to generate + load it if absent), fetches
# the offline fonts, builds the served index.html from ui.html, starts
# server.py, waits for the four warmup lines, then prints the URL and the
# SSH-tunnel line.  Ctrl-C stops the server.
#
# env overrides: BIN, SRC, WS, PORT   (see README.md)
# =============================================================================
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$DEMO_DIR/.." && pwd)"

BIN=${BIN:-$REPO_ROOT/build-release/tools/turbolynx}
SRC=${SRC:-/data/ladder-v7-src}
WS=${WS:-/data/ladder-v7-ws}
PORT=${PORT:-8500}
# Everything the browser may fetch is built into this dir and nothing else,
# so the server never exposes the repo tree it lives in.
WEBROOT="$DEMO_DIR/.run"
REGEN=0
FONTS=1
ASSUME_YES=${ASSUME_YES:-0}

while [ $# -gt 0 ]; do
    case "$1" in
        --regen)    REGEN=1 ;;
        --no-fonts) FONTS=0 ;;
        --yes|-y)   ASSUME_YES=1 ;;
        --port)     PORT="$2"; shift ;;
        --port=*)   PORT="${1#*=}" ;;
        -h|--help)  awk 'NR>1 && /^#/{sub(/^# ?/,"");print;next} NR>1{exit}' \
                        "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown flag: $1 (try --help)" >&2; exit 2 ;;
    esac
    shift
done

LOG="$DEMO_DIR/server.log"
SRV_PID=""
cleanup() { [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# ------------------------------------------------------------------ 0. binary
if [ ! -x "$BIN" ]; then
    echo "ERROR: turbolynx binary not found at $BIN" >&2
    echo "       build it: cd $REPO_ROOT/build-release && ninja" >&2
    exit 1
fi

# -------------------------------------------------- 1. data (generate + load)
# A loaded workspace always has a catalog file; an empty/half-written directory
# does not count as one.
ws_ok() { [ -d "$WS" ] && [ -n "$(ls -A "$WS" 2>/dev/null)" ]; }

if [ "$REGEN" = 1 ] || ! ws_ok; then
    if [ "$REGEN" = 1 ]; then
        say "--regen: rebuilding the dataset from scratch"
    else
        say "no workspace at $WS"
    fi
    echo "This builds the booth dataset:"
    echo "  source     $SRC     ~529 MB   ~4 min   (python3 gen_data.py)"
    echo "  workspace  $WS      ~2.1 GB   ~6 min   (turbolynx import)"
    echo "Total ~2.6 GB of disk and ~10 minutes.  Free on that filesystem:" \
         "$(df -h "$(dirname "$WS")" 2>/dev/null | awk 'NR==2{print $4}')"
    if [ "$ASSUME_YES" != 1 ] && [ -t 0 ]; then
        read -r -p "Proceed? [Y/n] " ans
        case "${ans:-y}" in [Nn]*) echo "aborted."; exit 1 ;; esac
    fi

    if [ "$REGEN" = 1 ] || [ ! -f "$SRC/nodes.json" ]; then
        say "generating source data -> $SRC"
        rm -rf "$SRC"
        python3 "$DEMO_DIR/gen_data.py" "$SRC"
    else
        echo "source data already present at $SRC (use --regen to rebuild)"
    fi
    say "loading workspace -> $WS"
    BIN="$BIN" SRC="$SRC" WS="$WS" bash "$DEMO_DIR/load_data.sh"
else
    echo "workspace present: $WS ($(du -sh "$WS" 2>/dev/null | cut -f1))"
fi

if [ ! -f "$SRC/profile_cols.txt" ]; then
    echo "ERROR: $SRC/profile_cols.txt missing — the server needs the 200" >&2
    echo "       profile column list that gen_data.py writes next to the data." >&2
    exit 1
fi

# ----------------------------------------------------------------- 2. fonts
# The booth is offline, so the page must not depend on fonts.googleapis.com.
mkdir -p "$WEBROOT"
if [ "$FONTS" = 1 ] && [ ! -f "$WEBROOT/fonts.css" ]; then
    say "fetching offline fonts (~3 MB, needs internet once)"
    if ! WEBROOT="$WEBROOT" python3 "$DEMO_DIR/fetch_fonts.py"; then
        echo "WARNING: font fetch failed — falling back to the Google Fonts CDN" >&2
        echo "         (the page then needs internet when it is displayed)." >&2
        FONTS=0
    fi
elif [ "$FONTS" = 1 ]; then
    echo "offline fonts present ($(ls "$WEBROOT/fonts" | wc -l) woff2 files)"
fi

# --------------------------------------------- 3. build the served index.html
# ui.html is the master and keeps the Google Fonts <link>s (it doubles as a
# standalone artifact).  The served copy gets a doctype/charset/viewport
# wrapper, and with local fonts the CDN links are swapped for fonts.css.
say "building index.html from ui.html"
OUT="$WEBROOT/index.html"
{
    printf '<!doctype html>\n<meta charset="utf-8">\n'
    printf '<meta name="viewport" content="width=device-width,initial-scale=1">\n'
    if [ "$FONTS" = 1 ]; then
        sed -e 's|^<link rel="preconnect".*|<link rel="stylesheet" href="fonts.css">|' \
            -e '/fonts\.googleapis\.com\/css2/d' "$DEMO_DIR/ui.html"
    else
        cat "$DEMO_DIR/ui.html"
    fi
} > "$OUT"

if [ "$FONTS" = 1 ]; then
    grep -q 'fonts.googleapis.com' "$OUT" && \
        { echo "ERROR: google font links survived the swap" >&2; exit 1; }
    grep -q 'href="fonts.css"' "$OUT" || \
        { echo "ERROR: fonts.css link missing from $OUT" >&2; exit 1; }
fi
# the 87 KB graph sample is the one thing that must never be re-encoded
grep -q 'const DATA = ' "$OUT" || \
    { echo "ERROR: DATA line missing from $OUT" >&2; exit 1; }
echo "index.html: $(wc -c < "$OUT") bytes, DATA md5 $(grep 'const DATA' "$OUT" | md5sum | cut -c1-8)"

# --------------------------------------------------------------- 4. serve
say "starting server on :$PORT"
: > "$LOG"
BIN="$BIN" WS="$WS" SRC="$SRC" PORT="$PORT" WEBROOT="$WEBROOT" \
    python3 -u "$DEMO_DIR/server.py" >>"$LOG" 2>&1 &
SRV_PID=$!

# --------------------------------------- 5. wait for the four warmup rungs
# Warmup runs base|si|gem|ssrf once each: it fills the display caches and,
# mainly, starts each mode's long-lived engine process and pays that process's
# one-off cold load there instead of on a booth click.  ~80 s in total.  Until
# it finishes the first booth click is slow, not broken.
echo "warming up the four rungs (~80 s; each line is one rung)"
DEADLINE=$(( $(date +%s) + 900 ))
while :; do
    kill -0 "$SRV_PID" 2>/dev/null || { echo "ERROR: server died:" >&2;
                                        tail -20 "$LOG" >&2; exit 1; }
    n=$(grep -c '^\[warmup\]' "$LOG" 2>/dev/null || true)
    [ "${n:-0}" -ge 4 ] && break
    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
        echo "ERROR: warmup did not finish in 15 min ($n/4 rungs):" >&2
        tail -20 "$LOG" >&2; exit 1
    fi
    sleep 2
done
grep '^\[warmup\]' "$LOG" | sed 's/^/  /'
if grep -q '^\[warmup\].*failed' "$LOG"; then
    echo "ERROR: at least one rung failed to warm up (see $LOG)" >&2
    exit 1
fi

HOSTNAME_S=$(hostname -f 2>/dev/null || hostname)
cat <<EOF

============================================================================
  BOOTH DEMO READY          http://localhost:$PORT/
============================================================================
  On this machine        : open http://localhost:$PORT/
  From your laptop       : ssh -N -L $PORT:localhost:$PORT ${USER:-root}@$HOSTNAME_S
                           then open http://localhost:$PORT/
  Design-only preview    : http://localhost:$PORT/?mock=1   (no engine calls)

  Five clicks: Run baseline -> Prune schema -> Split per district ->
               Pack rows -> Verify results.
  Measured execute times 3384 -> 1831 -> 647 -> 331 ms (10.2x end to end);
  every rung returns the same 33,452 venues / 167,260 rows.
  A click is one compile + one warm execution: ~3.9 s base, ~2.3 s SI,
  ~1.0 s GEM, ~0.8 s SSRF.  The page prints compile and execute separately.

  server log: $LOG        Ctrl-C to stop.
============================================================================
EOF

wait "$SRV_PID"
