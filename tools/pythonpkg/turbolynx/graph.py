"""Graph-specific query builders for TurboLynx.

Provides NodeQuery and PathQuery classes that generate Cypher queries
under the hood and return TurboLynxRelation objects for further chaining.
"""

from .relation import TurboLynxRelation


def _format_value(val):
    """Format a Python value as a Cypher literal."""
    if val is None:
        return 'NULL'
    if isinstance(val, bool):
        return 'true' if val else 'false'
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(val)
    if isinstance(val, str):
        escaped = val.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    return str(val)


def _format_props(props):
    """Format property dict as Cypher inline properties {key: value, ...}."""
    if not props:
        return ''
    parts = [f'{k}: {_format_value(v)}' for k, v in props.items()]
    return ' {' + ', '.join(parts) + '}'


def _rel_pattern(rel_type=None, direction="both", var="r", hops=None):
    """Build a relationship pattern string.

    Args:
        rel_type: Relationship type name (e.g., "KNOWS") or None for any
        direction: "out", "in", or "both"
        var: Variable name for the relationship
        hops: Tuple (min, max) for variable-length, or None for single hop
    """
    type_str = f':{rel_type}' if rel_type else ''
    if hops:
        min_h, max_h = hops
        if max_h is None:
            hop_str = f'*{min_h}..'
        else:
            hop_str = f'*{min_h}..{max_h}'
        type_str += hop_str

    rel = f'[{var}{type_str}]'
    if direction == "out":
        return f'-{rel}->'
    elif direction == "in":
        return f'<-{rel}-'
    else:
        return f'-{rel}-'


class NodeQuery:
    """Graph node query builder.

    Usage:
        conn.node("Person", firstName="Marc").neighbors("KNOWS").limit(5).fetchdf()
        conn.node("Person").degree("KNOWS", direction="out").order("degree DESC").fetchdf()
    """

    def __init__(self, conn, label, **props):
        self._conn = conn
        self._label = label
        self._props = props
        self._where_clauses = []

    def where(self, expr):
        """Add a WHERE condition on the source node (referenced as 'n').

        Example: conn.node("Person").where("n.age > 30")
        """
        nq = NodeQuery(self._conn, self._label, **self._props)
        nq._where_clauses = list(self._where_clauses) + [expr]
        return nq

    def _match_clause(self, var="n"):
        """Build the MATCH (n:Label {props}) WHERE ... fragment."""
        props_str = _format_props(self._props)
        match = f'MATCH ({var}:{self._label}{props_str})'
        if self._where_clauses:
            where = ' AND '.join(self._where_clauses)
            match += f' WHERE {where}'
        return match

    def properties(self, *cols):
        """Return specified properties of matching nodes as a Relation.

        Example: conn.node("Person", firstName="Marc").properties("firstName", "lastName", "id")
        """
        match = self._match_clause()
        if cols:
            ret_cols = ', '.join(f'n.{c}' for c in cols)
        else:
            # Get all properties from schema
            try:
                schema = self._conn.schema(self._label)
                ret_cols = ', '.join(f'n.{k}' for k in schema.keys())
            except Exception:
                ret_cols = 'n'
        query = f'{match} RETURN {ret_cols}'
        return TurboLynxRelation(self._conn, query)

    def count(self):
        """Count matching nodes.

        Example: conn.node("Person").count().fetchall()
        """
        match = self._match_clause()
        query = f'{match} RETURN count(n) AS count'
        return TurboLynxRelation(self._conn, query)

    def neighbors(self, rel_type=None, direction="both", target_label=None):
        """Traverse to neighboring nodes via a relationship.

        Returns a Relation with neighbor node properties.

        Args:
            rel_type: Relationship type (e.g., "KNOWS"). None for any type.
            direction: "out", "in", or "both"
            target_label: Optional target node label filter

        Example:
            conn.node("Person", firstName="Marc").neighbors("KNOWS").limit(5).fetchdf()
            conn.node("Person", firstName="Marc").neighbors("KNOWS", direction="out").fetchdf()
        """
        match = self._match_clause()
        rel = _rel_pattern(rel_type, direction, var="r")
        target = f'(m:{target_label})' if target_label else '(m)'
        query = f'{match}{rel}{target} RETURN m'
        return TurboLynxRelation(self._conn, query)

    def edges(self, rel_type=None, direction="both", target_label=None):
        """Return relationships (edges) from matching nodes.

        Returns a Relation with relationship properties.

        Example:
            conn.node("Person", firstName="Marc").edges("KNOWS").limit(5).fetchall()
        """
        match = self._match_clause()
        rel = _rel_pattern(rel_type, direction, var="r")
        target = f'(m:{target_label})' if target_label else '(m)'
        # Return edge properties
        try:
            if rel_type:
                schema = self._conn.schema(rel_type, edge=True)
                # Exclude internal _sid/_tid
                edge_cols = [f'r.{k}' for k in schema.keys()
                             if not k.startswith('_')]
                if edge_cols:
                    ret = ', '.join(edge_cols)
                else:
                    ret = 'r'
            else:
                ret = 'r'
        except Exception:
            ret = 'r'
        query = f'{match}{rel}{target} RETURN {ret}'
        return TurboLynxRelation(self._conn, query)

    def degree(self, rel_type=None, direction="both"):
        """Calculate the degree (number of relationships) for matching nodes.

        Returns a Relation with node identifier and degree count.

        Args:
            rel_type: Relationship type to count. None for all types.
            direction: "out" (out-degree), "in" (in-degree), or "both" (total degree)

        Example:
            conn.node("Person").degree("KNOWS").order("degree DESC").limit(10).fetchdf()
        """
        match = self._match_clause()
        rel = _rel_pattern(rel_type, direction, var="r")

        # Get a representative property for node identification
        try:
            schema = self._conn.schema(self._label)
            id_col = 'id' if 'id' in schema else list(schema.keys())[0]
        except Exception:
            id_col = 'id'

        query = f'{match}{rel}() RETURN n.{id_col} AS node_id, count(r) AS degree'
        return TurboLynxRelation(self._conn, query)


class PathQuery:
    """Path query builder for shortest path and variable-length traversals.

    Usage:
        conn.shortest_path("Person", "Person", rel="KNOWS",
                           source_filter="a.firstName = 'Marc'",
                           target_filter="b.firstName = 'John'").fetchall()
    """

    def __init__(self, conn, source_label, target_label, rel=None,
                 min_hops=1, max_hops=None, all_paths=False,
                 source_filter=None, target_filter=None, direction="both"):
        self._conn = conn
        self._source_label = source_label
        self._target_label = target_label
        self._rel = rel
        self._min_hops = min_hops
        self._max_hops = max_hops
        self._all_paths = all_paths
        self._source_filter = source_filter
        self._target_filter = target_filter
        self._direction = direction

    def _build_query(self):
        """Generate the Cypher path query."""
        rel_type = f':{self._rel}' if self._rel else ''
        if self._max_hops is not None:
            hops = f'*{self._min_hops}..{self._max_hops}'
        else:
            hops = f'*{self._min_hops}..'

        # Build relationship pattern
        rel_pattern = f'[{rel_type}{hops}]'
        if self._direction == "out":
            pattern = f'(a:{self._source_label})-{rel_pattern}->(b:{self._target_label})'
        elif self._direction == "in":
            pattern = f'(a:{self._source_label})<-{rel_pattern}-(b:{self._target_label})'
        else:
            pattern = f'(a:{self._source_label})-{rel_pattern}-(b:{self._target_label})'

        # Path function
        func = 'allShortestPaths' if self._all_paths else 'shortestPath'
        match = f'MATCH path = {func}({pattern})'

        # WHERE clause
        conditions = []
        if self._source_filter:
            conditions.append(self._source_filter)
        if self._target_filter:
            conditions.append(self._target_filter)

        where = ''
        if conditions:
            where = ' WHERE ' + ' AND '.join(conditions)

        return f'{match}{where} RETURN path, length(path) AS hops'

    def as_relation(self):
        """Convert to a TurboLynxRelation for further chaining."""
        return TurboLynxRelation(self._conn, self._build_query())

    # Terminal methods — delegate to relation
    def fetchall(self):
        return self.as_relation().fetchall()

    def fetchdf(self):
        return self.as_relation().fetchdf()

    def df(self):
        return self.fetchdf()

    def fetchone(self):
        return self.as_relation().fetchone()

    def limit(self, n):
        return self.as_relation().limit(n)

    def order(self, expr):
        return self.as_relation().order(expr)

    def show(self, n=20):
        return self.as_relation().show(n)

    @property
    def query(self):
        return self._build_query()

    def __repr__(self):
        return repr(self.as_relation())

    def __iter__(self):
        return iter(self.as_relation())
