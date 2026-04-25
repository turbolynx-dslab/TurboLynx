/**
 * Hard-coded scenario driving the MVP UI. Scripted turns synthesise what a
 * live LLM + MCP + Python sandbox would produce — voice and cadence tuned
 * to match how Sonnet actually behaves in practice: short plan, one tool
 * call, one paragraph interpreting the result, then the next tool.
 *
 * The workspace is a DBpedia cinema subgraph loaded under the single `NODE`
 * label. Types (Film, Person, Award, …) are not labels — they're edges to
 * peer nodes, which is why Step 1's agent uses `list_types` / `describe_type`
 * / `sample_type` rather than `list_labels`.
 */

export type Role = "user" | "agent";

export type ToolKind =
  | "list_types"
  | "describe_type"
  | "sample_type"
  | "list_labels"
  | "describe_label"
  | "sample_label"
  | "query_cypher"
  | "mutate_cypher"
  | "python";

export interface ToolCall {
  kind: ToolKind;
  title: string;
  payload?: string;
  resultHint?: string;
}

export interface ChatTurn {
  role: Role;
  text: string;
  tools?: ToolCall[];
}

export type VizKind =
  | "bars+cards"
  | "force-directed"
  | "force-directed+community"
  | "force-directed+centrality"
  | "sankey+heatmap"
  | "report";

export interface DBState {
  nodes: number;
  edges: number;
  filmNodes: number;
  recentQueries: string[];
}

export interface ScenarioStep {
  id: number;
  title: string;
  userPrompt: string;
  chat: ChatTurn[];
  viz: VizKind;
  dbState: DBState;
}

export const INITIAL_DB: DBState = {
  nodes: 0,
  edges: 0,
  filmNodes: 0,
  recentQueries: [],
};

export const STEPS: ScenarioStep[] = [
  {
    id: 1,
    title: "Data",
    userPrompt: "What's in this cinema data?",
    chat: [
      { role: "user", text: "What's in this cinema data?" },
      {
        role: "agent",
        text:
          "Good starting point — I have no prior knowledge of this workspace. It was loaded as a single NODE label (DBpedia is schemaless), so types live on rdf:type edges rather than graph labels. Let me walk those edges to see what types actually exist.",
        tools: [
          {
            kind: "list_types",
            title: "list_types()",
            payload: "MATCH (n)-[:type]->(t) RETURN t.uri AS uri, count(n) AS n ORDER BY n DESC LIMIT 50",
            resultHint: "26 types · Person 1,482 · Film 420 · Place 276 · Organisation 198 · Writer 152 · City 118 · MusicalArtist 67 · Genre 42 · Award 38 · Country 25 · …",
          },
        ],
      },
      {
        role: "agent",
        text:
          "26 distinct types. Person dominates (1,482), then Film (420), Place (276). A long tail of sub-types — Writer, MusicalArtist, Actor — so a lot of the Person bucket is role-specialised. Film is the narrative centre of this graph, so let me probe it first to see how uniform its property set is.",
        tools: [
          {
            kind: "describe_type",
            title: "describe_type(\"http://dbpedia.org/ontology/Film\")",
            payload:
              "MATCH (n)-[:type]->(t {uri:'http://dbpedia.org/ontology/Film'})\nUNWIND keys(n) AS k\nRETURN k AS property, count(*) AS present\nORDER BY present DESC",
            resultHint: "420 films · name 100% · releaseDate 71% · director 87% · starring 63% · runtime 62% · country 58% · genre 41% · writer 31% · budget 14% · gross 11%",
          },
        ],
      },
      {
        role: "agent",
        text:
          "Exactly what you'd expect from an open knowledge graph — wildly non-uniform. Every Film has a name; most have a director; runtime drops to 62 %, budget to 14 %. A label-based system would hide this by forcing one schema on the whole partition. Here it's visible, which matters for the analysis I run next. Let me grab a few concrete films so the property bag isn't abstract.",
        tools: [
          {
            kind: "sample_type",
            title: "sample_type(\"http://dbpedia.org/ontology/Film\", 3)",
            payload:
              "MATCH (n)-[:type]->(t {uri:'http://dbpedia.org/ontology/Film'})\nRETURN n LIMIT 3",
            resultHint: "Inception · Mulholland Drive · The Godfather Part II",
          },
        ],
      },
      {
        role: "agent",
        text:
          "Three films with noticeably different property bags. Inception carries budget and gross; Mulholland Drive carries cinematography but no gross; The Godfather Part II has writer + editing but no musicComposer. So the data is rich but sparse, and any analysis I do has to cope with missing fields. Right-side panel shows top-level counts. Ready for the next question.",
      },
    ],
    viz: "bars+cards",
    dbState: {
      nodes: 2_841,
      edges: 9_712,
      filmNodes: 420,
      recentQueries: [
        "MATCH (n)-[:type]->(t) RETURN t.uri, count(n)",
        "MATCH (n)-[:type]->(t {uri:$u}) UNWIND keys(n) AS k RETURN k, count(*)",
      ],
    },
  },

  {
    id: 2,
    title: "Network",
    userPrompt: "Show me Best Director winners and their collaborators as a network.",
    chat: [
      { role: "user", text: "Show me Best Director winners and their collaborators as a network." },
      {
        role: "agent",
        text:
          "One Cypher query should cover this — find directors with a Best-Director-style award, then fan out to everyone they share a credit with. I'll weight each edge by how many times those two people worked together.",
        tools: [
          {
            kind: "query_cypher",
            title: "query_cypher — directors + weighted collaborators",
            payload:
              "MATCH (d)-[:HAS_AWARD]->(a)\nWHERE a.name CONTAINS 'Best Director'\nMATCH (d)-[r:DIRECTED_BY|STARRING|CINEMATOGRAPHY_BY|PRODUCED_BY|EDITED_BY|WRITTEN_BY]-(x)\nRETURN d.name AS director, x.name AS collaborator,\n       type(r) AS relation, count(*) AS weight\nORDER BY weight DESC LIMIT 600",
            resultHint: "1,483 rows · 47 distinct directors · 864 unique collaborators",
          },
        ],
      },
      {
        role: "agent",
        text:
          "1,483 edges across 47 directors and ~860 collaborators. Comfortable size for a single force-directed layout. I'll load it into networkx, set the edge weights, then hand it to pyvis for rendering. The DOM canvas on the right has physics, so nodes repel and spring to each other; mousing over shows the name.",
        tools: [
          {
            kind: "python",
            title: "python — networkx + pyvis",
            payload:
              "import networkx as nx\nfrom pyvis.network import Network\n\nG = nx.Graph()\nfor r in rows:\n    G.add_edge(\n        r['director'], r['collaborator'],\n        weight=r['weight'], rel=r['relation'],\n    )\n\nnet = Network(height='560px', width='100%', bgcolor='#ffffff')\nnet.from_nx(G)\nnet.force_atlas_2based(spring_length=40, central_gravity=0.01)\nnet.show('collab.html')",
            resultHint: "rendered · 47 directors + 864 collaborators · density 0.003",
          },
        ],
      },
      {
        role: "agent",
        text:
          "Rendered. Spielberg, Scorsese, Kurosawa, Fellini are the most-connected hubs — each sits in a dense local cluster of recurring actors and craftspeople. The layout hasn't been coloured yet; it's a plain structural view. Next move is usually to ask what communities the structure breaks into.",
      },
    ],
    viz: "force-directed",
    dbState: {
      nodes: 2_841,
      edges: 9_712,
      filmNodes: 420,
      recentQueries: [
        "MATCH (d)-[:HAS_AWARD]->(a) WHERE a.name CONTAINS 'Best Director' …",
      ],
    },
  },

  {
    id: 3,
    title: "Communities",
    userPrompt: "Find the hidden groups in this network.",
    chat: [
      { role: "user", text: "Find the hidden groups in this network." },
      {
        role: "agent",
        text:
          "Community detection isn't something the database does — TurboLynx serves the graph, not analytical algorithms on top of it. The graph is already in Python memory from step 2, so I'll run Louvain modularity maximisation directly. Louvain is a good default here: parameter-free, weighted, handles disconnected components.",
        tools: [
          {
            kind: "python",
            title: "python — Louvain on the in-memory graph",
            payload:
              "import community as community_louvain\n\n# G is still in memory from the previous step\npartition = community_louvain.best_partition(G, weight='weight', random_state=42)\n# partition: { node_name -> community_id }\n\nfrom collections import Counter\nsizes = Counter(partition.values()).most_common()\nprint('modularity =', community_louvain.modularity(partition, G))\nprint('sizes:', sizes[:5])",
            resultHint: "24 communities · modularity 0.61 · top-5 sizes 87 / 63 / 41 / 34 / 29",
          },
        ],
      },
      {
        role: "agent",
        text:
          "24 communities with modularity 0.61 — a solid clustering signal. The three biggest buckets hold 87, 63, 41 nodes; probably a mix of era-based and studio-based groupings. I'm keeping the partition dict in my Python runtime so later steps can join against it without re-running detection. Re-rendering the same layout with a colour per community so the structure pops.",
      },
    ],
    viz: "force-directed+community",
    dbState: {
      nodes: 2_841,
      edges: 9_712,
      filmNodes: 420,
      recentQueries: ["(no DB call — Python only)"],
    },
  },

  {
    id: 4,
    title: "Centrality",
    userPrompt: "Who sits at the centre of each group?",
    chat: [
      { role: "user", text: "Who sits at the centre of each group?" },
      {
        role: "agent",
        text:
          "PageRank on the same weighted graph, then I'll group scores by the community assignment from step 3 and take the top-3 per group. Still no DB call — the graph and the partition are both already in Python.",
        tools: [
          {
            kind: "python",
            title: "python — weighted PageRank",
            payload:
              "import pandas as pd\n\npagerank = nx.pagerank(G, weight='weight')\n\ndf = (pd.Series(pagerank).rename('pr')\n        .reset_index().rename(columns={'index':'name'}))\ndf['community'] = df['name'].map(partition)\n\ntop3 = (df.sort_values(['community', 'pr'], ascending=[True, False])\n          .groupby('community')\n          .head(3))",
            resultHint: "top hubs · Scorsese (c0) · Spielberg (c1) · Kurosawa (c2) · Fellini (c3) · Almodóvar (c4) …",
          },
        ],
      },
      {
        role: "agent",
        text:
          "Node radii on the canvas now scale with PageRank score, so central figures pop immediately. Scorsese anchors the first community with De Niro and Pesci close behind; Spielberg anchors another with Ford and Williams nearby; Kurosawa sits at the centre of his own tight cluster. The overlay is additive — same layout, new size channel, nothing re-computed in the DB.",
      },
    ],
    viz: "force-directed+centrality",
    dbState: {
      nodes: 2_841,
      edges: 9_712,
      filmNodes: 420,
      recentQueries: ["(no DB call — Python only)"],
    },
  },

  {
    id: 5,
    title: "Flow",
    userPrompt: "How does collaboration flow between communities over time?",
    chat: [
      { role: "user", text: "How does collaboration flow between communities over time?" },
      {
        role: "agent",
        text:
          "Two parts. First a Cypher pull for director–actor co-participation with release year — I need the time axis, and I want it at the edge granularity rather than pre-aggregated. Then in Python I join that against the community assignment still resident from step 3 and build the Sankey + heatmap.",
        tools: [
          {
            kind: "query_cypher",
            title: "query_cypher — director × actor co-participation by year",
            payload:
              "MATCH (p1)-[:DIRECTED_BY]->(f)<-[:STARRING]-(p2)\nWHERE p1 <> p2 AND f.releaseYear IS NOT NULL\nRETURN p1.name AS director,\n       p2.name AS actor,\n       f.releaseYear AS year,\n       count(*) AS collaborations\nORDER BY year",
            resultHint: "6,217 rows · years 1948–2024 · 47 directors · 312 actors",
          },
        ],
      },
      {
        role: "agent",
        text:
          "6,217 director–actor pairs, spanning 76 years. Now I map each endpoint through the partition dict to get a (community_a, community_b) pair, aggregate for the Sankey, and bin by year for the heatmap. Both are first-of-their-kind visualisations in this session — neither exists in the DB, both are generated here on the fly.",
        tools: [
          {
            kind: "python",
            title: "python — Sankey + yearly heatmap",
            payload:
              "import pandas as pd\nimport plotly.graph_objects as go\n\ndf = pd.DataFrame(rows)\ndf['c1'] = df['director'].map(partition)\ndf['c2'] = df['actor'].map(partition)\ndf = df.dropna(subset=['c1', 'c2'])\n\n# Sankey — community × community flows\nflow = df.groupby(['c1', 'c2'])['collaborations'].sum().reset_index()\nnodes = sorted(set(flow.c1) | set(flow.c2))\nidx = {c: i for i, c in enumerate(nodes)}\nfig = go.Figure(go.Sankey(\n    node=dict(label=[f'community {c}' for c in nodes]),\n    link=dict(\n        source=flow.c1.map(idx),\n        target=flow.c2.map(idx),\n        value=flow.collaborations,\n    ),\n))\n\n# Heatmap — year × community-pair\nheat = (df.groupby(['year', 'c1', 'c2'])['collaborations']\n          .sum().unstack(fill_value=0))",
            resultHint: "Sankey rendered · heatmap rendered (20 years × 12 community-pairs)",
          },
        ],
      },
      {
        role: "agent",
        text:
          "Sankey's on top, heatmap below. A couple of readings: within-community collaboration dominates the flows (fat self-loops on the diagonal), and cross-community bridges are rare but bursty — the heatmap shows clusters in the early 2000s and mid-2010s, corresponding to studio consolidations and a few directors who moved between cliques. That's the kind of pattern the raw edge list doesn't surface on its own.",
      },
    ],
    viz: "sankey+heatmap",
    dbState: {
      nodes: 2_841,
      edges: 9_712,
      filmNodes: 420,
      recentQueries: [
        "MATCH (p1)-[:DIRECTED_BY]->(f)<-[:STARRING]-(p2) …",
      ],
    },
  },

  {
    id: 6,
    title: "Report",
    userPrompt: "Write up everything we've seen.",
    chat: [
      { role: "user", text: "Write up everything we've seen." },
      {
        role: "agent",
        text:
          "I'll assemble every user turn, every Cypher statement we ran, the Python snippets I wrote this session, and the rendered figures into a single docx. Nothing DB-side for this — python-docx handles the layout. The cleanup / audit-trail section gets its own page so the writes are traceable.",
        tools: [
          {
            kind: "python",
            title: "python — docx compiler",
            payload:
              "from docx import Document\nfrom docx.shared import Inches\n\ndoc = Document()\ndoc.add_heading('Cinema Graph — Exploration Report', level=0)\ndoc.add_paragraph(f'Generated {today} · turbolynx.io · dbpedia-cinema')\n\nfor step in session.steps:\n    doc.add_heading(step.title, level=1)\n    doc.add_paragraph(step.user_utterance, style='Intense Quote')\n    for cypher in step.cyphers:\n        doc.add_paragraph(cypher, style='Intense Quote')\n    for py in step.python_snippets:\n        doc.add_paragraph(py, style='Intense Quote')\n    for fig in step.figures:\n        doc.add_picture(fig.png, width=Inches(6.0))\n\ndoc.save('/tmp/agentic_report.docx')",
            resultHint: "report saved · 6 sections · 24 pages · 5 embedded figures",
          },
        ],
      },
      {
        role: "agent",
        text:
          "Two-page preview on the canvas, with the full 24-page docx written out to /tmp/agentic_report.docx. Every DB call and every code block the agent emitted is in there verbatim, so the session is fully reproducible from the report alone. Worth pointing out: the report itself is another piece of code the agent wrote this session — the database did not do any of this assembly.",
      },
    ],
    viz: "report",
    dbState: {
      nodes: 2_841,
      edges: 9_712,
      filmNodes: 420,
      recentQueries: ["(no DB call — Python only)"],
    },
  },
];
