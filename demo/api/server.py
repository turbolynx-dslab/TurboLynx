#!/usr/bin/env python3
"""TurboLynx Demo API — bridges the demo frontend to turbolynx CLI + Claude NL2Cypher."""

import json
import os
import subprocess
import re
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="TurboLynx Demo API")

# --- CORS (allow demo frontend) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://turbolynx.io",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8000",
        "http://127.0.0.1:8001",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Config ---
TURBOLYNX_BIN = os.environ.get("TURBOLYNX_BIN", os.path.expanduser("~/bin/turbolynx"))
WORKSPACE = os.environ.get("TURBOLYNX_WORKSPACE", os.path.expanduser("~/data/ldbc/sf1"))
TURBOLYNX_LIB = os.environ.get("TURBOLYNX_LIB", os.path.expanduser("~/bin"))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")


# --- Models ---
class QueryRequest(BaseModel):
    cypher: str
    timeout: int = 30  # seconds


class NL2CypherRequest(BaseModel):
    question: str


class NL2CypherResponse(BaseModel):
    cypher: str
    explanation: str = ""


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list]
    elapsed_ms: float = 0
    cypher: str = ""


# --- Schema for NL2Cypher context ---
LDBC_SCHEMA = """
Node labels: Person, Comment, Post, Forum, Organisation, Place, Tag, TagClass
Edge types: KNOWS, HAS_CREATOR, HAS_TAG, IS_LOCATED_IN, IS_PART_OF, IS_SUBCLASS_OF,
  CONTAINER_OF, HAS_MEMBER, HAS_MODERATOR, HAS_TYPE, LIKES, REPLY_OF, STUDY_AT, WORK_AT

Person properties: id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed
Comment properties: id, content, length, creationDate, locationIP, browserUsed
Post properties: id, content, imageFile, length, language, creationDate, locationIP, browserUsed
Forum properties: id, title, creationDate
Organisation properties: id, name, url, type
Place properties: id, name, url, type
Tag properties: id, name, url
TagClass properties: id, name, url

KNOWS: Person->Person (creationDate)
HAS_CREATOR: Comment|Post->Person
HAS_TAG: Comment|Post|Forum->Tag
IS_LOCATED_IN: Person|Comment|Post|Organisation->Place
IS_PART_OF: Place->Place
IS_SUBCLASS_OF: TagClass->TagClass
CONTAINER_OF: Forum->Post
HAS_MEMBER: Forum->Person (joinDate)
HAS_MODERATOR: Forum->Person
HAS_TYPE: Tag->TagClass
LIKES: Person->Comment|Post (creationDate)
REPLY_OF: Comment->Comment|Post
STUDY_AT: Person->Organisation (classYear)
WORK_AT: Person->Organisation (workFrom)
"""


# --- Endpoints ---
@app.get("/api/health")
def health():
    bin_exists = os.path.isfile(TURBOLYNX_BIN)
    ws_exists = os.path.isdir(WORKSPACE)
    return {"status": "ok", "binary": bin_exists, "workspace": ws_exists, "has_api_key": bool(ANTHROPIC_API_KEY)}


@app.post("/api/query", response_model=QueryResponse)
def run_query(req: QueryRequest):
    """Execute a Cypher query via turbolynx CLI and return structured results."""
    if not os.path.isfile(TURBOLYNX_BIN):
        raise HTTPException(500, f"turbolynx binary not found at {TURBOLYNX_BIN}")
    if not os.path.isdir(WORKSPACE):
        raise HTTPException(500, f"workspace not found at {WORKSPACE}")

    cmd = [
        TURBOLYNX_BIN,
        "--workspace", WORKSPACE,
        "--query", req.cypher,
        "--log-level", "error",
    ]
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = TURBOLYNX_LIB + ":" + env.get("LD_LIBRARY_PATH", "")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=req.timeout, env=env)
    except subprocess.TimeoutExpired:
        raise HTTPException(408, "Query timed out")

    if result.returncode != 0:
        raise HTTPException(400, f"Query error: {result.stderr.strip()}")

    # Parse table-format output from turbolynx
    # Format:
    #   catalog_version: ...
    #   catalog: restored ...
    #   +--------+--------+
    #   | col1   | col2   |
    #   +--------+--------+
    #   | val1   | val2   |
    #   +--------+--------+
    #   N rows
    #   Time: compile X ms, execute Y ms, total Z ms
    stdout = result.stdout
    columns = []
    rows = []
    elapsed = 0.0

    # Extract timing
    m = re.search(r"total\s+(\d+\.?\d*)\s*ms", stdout)
    if m:
        elapsed = float(m.group(1))

    # Find table rows (lines starting with |)
    table_lines = [l.strip() for l in stdout.split("\n") if l.strip().startswith("|")]
    if table_lines:
        # First | line is header
        columns = [c.strip() for c in table_lines[0].strip("|").split("|")]
        # Remaining | lines are data rows
        for line in table_lines[1:]:
            row = [c.strip() for c in line.strip("|").split("|")]
            rows.append(row)

    return QueryResponse(columns=columns, rows=rows, elapsed_ms=elapsed, cypher=req.cypher)


@app.post("/api/nl2cypher", response_model=NL2CypherResponse)
def nl2cypher(req: NL2CypherRequest):
    """Convert natural language to Cypher using Claude CLI."""
    system_prompt = f"""You are a Cypher query generator for a graph database called TurboLynx.
Given a natural language question about the LDBC Social Network Benchmark dataset,
generate a valid Cypher query.

Database schema:
{LDBC_SCHEMA}

Rules:
- Use only the labels, types, and properties listed above.
- Return ONLY the Cypher query, no explanation.
- Use LIMIT 20 by default unless the user specifies otherwise.
- For variable-length paths use [*1..3] syntax.
"""

    prompt = f"{system_prompt}\n\nQuestion: {req.question}"
    try:
        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True, text=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(408, "NL2Cypher timed out")

    if result.returncode != 0:
        raise HTTPException(500, f"Claude CLI error: {result.stderr.strip()}")

    cypher = result.stdout.strip()
    # Strip markdown code fences if present
    cypher = re.sub(r"^```(?:cypher)?\s*\n?", "", cypher)
    cypher = re.sub(r"\n?```\s*$", "", cypher)

    return NL2CypherResponse(cypher=cypher.strip())


if __name__ == "__main__":
    import uvicorn
    ssl_cert = os.path.expanduser("~/api/cert.pem")
    ssl_key = os.path.expanduser("~/api/key.pem")
    ssl_kwargs = {}
    if os.path.isfile(ssl_cert) and os.path.isfile(ssl_key):
        ssl_kwargs = {"ssl_certfile": ssl_cert, "ssl_keyfile": ssl_key}
    uvicorn.run(app, host="0.0.0.0", port=8080, **ssl_kwargs)
