Hello. We'll be demonstrating LLM agents working with TurboLynx as an agentic database. We're not showing a new database feature today — we're showing a division of labour between the database and an autonomous agent.

An agentic database is the structure over which an agent plans, retrieves, and reasons. Plans, reasoning paths, and tool routing are inherently graph-shaped, so a schemaless graph engine is a natural fit.

Three parties interact. The user speaks natural language. The agent plans, writes Cypher, writes Python, picks the visualization. TurboLynx does three things and nothing more: reports schema, reads the graph, writes when asked. The dataset is a DBpedia movie subgraph — about twenty-eight hundred nodes and ninety-seven hundred edges, loaded under a single NODE label. DBpedia is schemaless, and we preserve that. Type membership is an edge, not a label, so the agent has to discover types at runtime.

Across the next six steps, keep count of two columns. On one side, what the database does — three capabilities. On the other side, what the agent delivers — six analyses. That ratio is the argument. The gap is filled entirely by code the agent writes in this session.

Let's walk through it.

The user opens with "what's in this cinema data?" The agent has no prior knowledge of the schema. It calls list_types, then describe_type on Film, then sample_type for three concrete rows. Twenty-six types. Film property coverage is wildly non-uniform — eighty-seven percent have a director, sixty-two percent a runtime, only fourteen percent a budget. That's schemaless in the wild. No Python yet. The database introduces itself.

Second question: "show me Academy Best Director winners and their collaborators as a network." One Cypher match returns one thousand four hundred eighty-three weighted edges across forty-seven directors. The agent loads those edges into networkx and renders an interactive force-directed layout with pyvis. The rendering code was written by the agent in this session.

Third: "find the hidden groups." This is the central move. The agent recognizes community detection. TurboLynx does not have community detection. The agent writes Python on the spot — Louvain modularity over the networkx graph already in memory. Twenty-four communities, modularity zero point six one. The network re-renders with community colours. Worth stating plainly: TurboLynx did not gain a feature. No MCP tool was added. Yet the user received a community-detection result. The difference came entirely from the agent generating Python. This is the mechanism of unbounded extension.

Fourth: "who's at the center?" Same pattern — networkx PageRank in Python, node radii on the canvas now scale with score. Scorsese anchors one community, Spielberg another. Same layout, new information overlaid.

Fifth: "how does collaboration flow between the communities over time?" The agent pulls director and actor co-participation with release year — six thousand two hundred seventeen rows. In Python it joins those rows against the community assignment still resident from step three, then builds two visualizations: a Sankey diagram of community-to-community flow on top, and a year-by-community-pair heatmap below. Both visualization types are first-of-their-kind in this session.

Sixth: "write this up." The agent assembles every user turn, every Cypher statement, every Python snippet, and the rendered figures into a single docx. Twenty-four pages, six sections. The report itself is another piece of code the agent wrote this session.

Across six steps TurboLynx did three things — report schema, read the graph, return structured results. No new database capability was added. Yet the user received six distinct kinds of analysis. Analytical reach is bounded not by the database's feature list, but by what the LLM can express in code — in practice unbounded. The database stays simple, the agent extends without limit, and the separation of roles makes that extension possible.

That concludes our demonstration. Thank you.
