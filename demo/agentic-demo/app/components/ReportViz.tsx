"use client";

/** Scene 6 — a scrollable pseudo-docx report the agent "wrote". Two A4-like
 *  pages stacked vertically, readable English text that summarises the
 *  session. No canvas — plain scrollable HTML. */
export default function ReportViz() {
  const today = new Date().toISOString().slice(0, 10);

  return (
    <div className="report-viz">
      <div className="report-page">
        <h1>Cinema Graph — Exploration Report</h1>
        <div className="report-page__meta">
          Generated {today} · turbolynx.io · dbpedia-cinema · 6 sections
        </div>

        <h2>1. Data overview</h2>
        <p>
          The workspace is a DBpedia cinema subgraph loaded under a single
          <code> NODE </code>label. Type membership is expressed through
          <code> rdf:type </code>edges. Discovering the schema required walking
          those edges and reading the resulting property coverage histograms.
        </p>
        <pre>{`26 types detected.
Top types by node count:
  Person  1,482    Film    420
  Place     276    Organisation 198
  Writer    152    City    118
Film property coverage:
  director      87 %
  releaseDate   71 %
  runtime       62 %
  budget        14 %`}</pre>

        <h2>2. Best-director collaborator network</h2>
        <p>
          A single Cypher query returned directors with a Best-Director-style
          award and every collaborator they share a film with. Result: 1,483
          rows across 47 directors. networkx loaded the edges; pyvis rendered
          an interactive force-directed layout.
        </p>
        <pre>{`MATCH (d)-[:HAS_AWARD]->(a) WHERE a.name CONTAINS 'Best Director'
MATCH (d)-[r:STARRING|PRODUCED_BY|CINEMATOGRAPHY_BY|DIRECTED_BY]-(x)
RETURN d.name AS director, x.name AS collaborator,
       type(r) AS relation, count(*) AS weight`}</pre>

        <h2>3. Community detection (Louvain)</h2>
        <p>
          No DB call for this step. Louvain modularity optimisation ran in
          Python against the in-memory networkx graph and yielded 24
          communities (sizes 87 / 63 / 41 / …). The same layout was
          re-rendered with community colours.
        </p>
        <pre>{`import community as community_louvain
partition = community_louvain.best_partition(G, weight='weight')`}</pre>
      </div>

      <div className="report-page">
        <h2>4. Centrality (PageRank)</h2>
        <p>
          Weighted PageRank, again in Python. Node sizes on the shared layout
          now scale with score. The ranking cards list the top-3 per
          community — directors with the tightest collaboration footprints
          surface naturally.
        </p>
        <pre>{`pagerank = nx.pagerank(G, weight='weight')
top3 = (pd.Series(pagerank)
          .rename('pr')
          .reset_index()
          .assign(c=lambda d: d['index'].map(partition))
          .groupby('c')
          .apply(lambda g: g.nlargest(3, 'pr')))`}</pre>

        <h2>5. Community-to-community flow</h2>
        <p>
          Co-participation edges with year were pulled from the graph, joined
          against the community assignment still resident in Python, and
          rendered as a Sankey diagram plus a year × community-pair heatmap.
          Both visualisations were first-of-their-kind in this session —
          generated on the fly by the agent.
        </p>
        <pre>{`MATCH (p1)-[:DIRECTED_BY]->(f)<-[:STARRING]-(p2)
WHERE p1 <> p2
RETURN p1.name AS director, p2.name AS actor,
       f.releaseYear AS year, count(*) AS collaborations`}</pre>

        <h2>6. What TurboLynx did vs. what the agent did</h2>
        <p>
          Over six steps TurboLynx answered three kinds of request: enumerate
          types, read the graph, and report schema. No new DB capability was
          added. Every analytical mode — force-directed rendering, Louvain,
          PageRank, Sankey, docx compilation — came from Python code the
          agent wrote in this session.
        </p>
        <p>
          Analytical reach is bounded not by the database's feature list,
          but by what the LLM can express in code. The database stays simple;
          the agent extends without limit.
        </p>
      </div>
    </div>
  );
}
