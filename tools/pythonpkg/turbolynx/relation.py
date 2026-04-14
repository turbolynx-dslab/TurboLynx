"""DuckDB-style lazy Relation API for TurboLynx.

Provides chainable query operations that defer execution until a terminal
method (fetchall, fetchdf, show, etc.) is called.
"""

from . import _cypher_rewriter as rewriter


class TurboLynxRelation:
    """Lazy, immutable Cypher query builder with DuckDB-style chaining.

    Usage:
        rel = conn.sql("MATCH (n:Person) RETURN n.name, n.age")
        result = rel.filter("n.age > 30").order("n.age DESC").limit(10).fetchdf()
    """

    def __init__(self, connection, base_query, *, _ops=None, _params=None):
        self._conn = connection
        self._base_query = base_query.strip().rstrip(';')
        self._ops = list(_ops or [])
        self._params = dict(_params or {})
        self._result_cache = None

    def _clone(self, extra_op=None):
        """Create a new Relation with an optional extra operation appended."""
        ops = list(self._ops)
        if extra_op:
            ops.append(extra_op)
        return TurboLynxRelation(self._conn, self._base_query,
                                _ops=ops, _params=self._params)

    # ------------------------------------------------------------------
    # Chaining methods (return new Relation)
    # ------------------------------------------------------------------

    def filter(self, expr):
        """Add a WHERE filter condition.

        Example: rel.filter("n.age > 30")
        """
        return self._clone(('FILTER', expr))

    def project(self, *cols):
        """Replace RETURN columns.

        Example: rel.project("n.name", "n.age")
        Or:      rel.project("n.name, n.age")
        """
        if len(cols) == 1 and ',' in cols[0]:
            col_str = cols[0]
        else:
            col_str = ', '.join(cols)
        return self._clone(('PROJECT', col_str))

    def order(self, expr):
        """Set ORDER BY clause.

        Example: rel.order("n.age DESC")
        """
        return self._clone(('ORDER', expr))

    def sort(self, expr):
        """Alias for order()."""
        return self.order(expr)

    def limit(self, n):
        """Set LIMIT clause.

        Example: rel.limit(10)
        """
        return self._clone(('LIMIT', int(n)))

    def skip(self, n):
        """Set SKIP clause.

        Example: rel.skip(5)
        """
        return self._clone(('SKIP', int(n)))

    def distinct(self):
        """Add DISTINCT to RETURN.

        Example: rel.distinct()
        """
        return self._clone(('DISTINCT',))

    def union(self, other):
        """Combine with another Relation using UNION ALL.

        Example: r1.union(r2)
        """
        if not isinstance(other, TurboLynxRelation):
            raise TypeError("union() requires a TurboLynxRelation")
        return self._clone(('UNION', other))

    def union_distinct(self, other):
        """Combine with another Relation using UNION (distinct).

        Example: r1.union_distinct(r2)
        """
        if not isinstance(other, TurboLynxRelation):
            raise TypeError("union_distinct() requires a TurboLynxRelation")
        return self._clone(('UNION_DISTINCT', other))

    def aggregate(self, aggr_exprs, group_by=None):
        """Apply aggregation.

        Example: rel.aggregate("count(*) AS cnt, avg(n.age) AS avg_age", "n.city")
        """
        return self._clone(('AGGREGATE', aggr_exprs, group_by))

    def count(self):
        """Replace RETURN with count(*).

        Example: rel.count()
        """
        return self._clone(('COUNT',))

    # ------------------------------------------------------------------
    # Query building
    # ------------------------------------------------------------------

    def _build_query(self):
        """Compose the final Cypher query by applying all operations."""
        query = self._base_query
        for op in self._ops:
            op_type = op[0]
            if op_type == 'FILTER':
                query = rewriter.apply_filter(query, op[1])
            elif op_type == 'PROJECT':
                query = rewriter.apply_project(query, op[1])
            elif op_type == 'ORDER':
                query = rewriter.apply_order(query, op[1])
            elif op_type == 'LIMIT':
                query = rewriter.apply_limit(query, op[1])
            elif op_type == 'SKIP':
                query = rewriter.apply_skip(query, op[1])
            elif op_type == 'DISTINCT':
                query = rewriter.apply_distinct(query)
            elif op_type == 'UNION':
                other_query = op[1]._build_query()
                query = f'{query} UNION ALL {other_query}'
            elif op_type == 'UNION_DISTINCT':
                other_query = op[1]._build_query()
                query = f'{query} UNION {other_query}'
            elif op_type == 'AGGREGATE':
                query = rewriter.apply_aggregate(query, op[1], op[2] if len(op) > 2 else None)
            elif op_type == 'COUNT':
                query = rewriter.apply_count(query)
        return query

    # ------------------------------------------------------------------
    # Terminal methods (trigger execution)
    # ------------------------------------------------------------------

    def _execute_cached(self):
        """Execute and cache the result."""
        if self._result_cache is None:
            self._result_cache = self.execute()
        return self._result_cache

    def execute(self):
        """Execute the query and return a TurboLynxPyResult."""
        q = self._build_query()
        if self._params:
            return self._conn.execute(q, self._params)
        return self._conn.execute(q)

    def fetchone(self):
        """Fetch a single row."""
        return self.execute().fetchone()

    def fetchmany(self, size=1):
        """Fetch multiple rows."""
        return self.execute().fetchmany(size)

    def fetchall(self):
        """Fetch all rows as a list of tuples."""
        return self.execute().fetchall()

    def fetchdf(self):
        """Fetch all rows as a pandas DataFrame."""
        return self.execute().fetchdf()

    def df(self):
        """Alias for fetchdf()."""
        return self.fetchdf()

    def fetchnumpy(self):
        """Fetch all rows as a dict of numpy arrays."""
        return self.execute().fetchnumpy()

    def show(self, n=20):
        """Print the first n rows as a formatted table, return self for chaining."""
        rel = self.limit(n) if n else self
        result = rel.execute()
        cols = [c.cast(str) if hasattr(c, 'cast') else str(c)
                for c in result.column_names()]
        rows = result.fetchall()

        if not cols:
            print("(empty result)")
            return self

        # Calculate column widths
        widths = [len(c) for c in cols]
        str_rows = []
        for row in rows:
            str_row = [str(v) if v is not None else 'NULL' for v in row]
            str_rows.append(str_row)
            for i, v in enumerate(str_row):
                widths[i] = max(widths[i], len(v))

        # Print header
        header = ' | '.join(c.ljust(widths[i]) for i, c in enumerate(cols))
        sep = '-+-'.join('-' * w for w in widths)
        print(header)
        print(sep)
        for row in str_rows:
            print(' | '.join(v.ljust(widths[i]) for i, v in enumerate(row)))

        total = len(rows)
        if n and total >= n:
            print(f"({total} rows, showing first {n})")
        else:
            print(f"({total} rows)")

        return self

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def query(self):
        """Return the built Cypher query string without executing."""
        return self._build_query()

    @property
    def columns(self):
        """Return column names by executing a LIMIT 1 probe."""
        probe = self.limit(1).execute()
        return [str(c) for c in probe.column_names()]

    @property
    def types(self):
        """Return column types by executing a LIMIT 1 probe."""
        probe = self.limit(1).execute()
        return [str(t) for t in probe.column_types()]

    @property
    def shape(self):
        """Return (row_count, col_count)."""
        result = self._execute_cached()
        ncols = len(result.column_names())
        nrows = result.rowcount if hasattr(result, 'rowcount') else len(result.fetchall())
        return (nrows, ncols)

    # ------------------------------------------------------------------
    # Dunder methods
    # ------------------------------------------------------------------

    def __repr__(self):
        try:
            result = self.limit(10).execute()
            cols = [str(c) for c in result.column_names()]
            rows = result.fetchall()

            if not cols:
                return "TurboLynxRelation(empty)"

            widths = [len(c) for c in cols]
            str_rows = []
            for row in rows:
                str_row = [str(v) if v is not None else 'NULL' for v in row]
                str_rows.append(str_row)
                for i, v in enumerate(str_row):
                    widths[i] = max(widths[i], len(v))

            lines = []
            header = ' | '.join(c.ljust(widths[i]) for i, c in enumerate(cols))
            sep = '-+-'.join('-' * w for w in widths)
            lines.append(header)
            lines.append(sep)
            for row in str_rows:
                lines.append(' | '.join(v.ljust(widths[i]) for i, v in enumerate(row)))

            return '\n'.join(lines)
        except Exception as e:
            return f"TurboLynxRelation(query={self._base_query!r}, error={e})"

    def __iter__(self):
        result = self.execute()
        while True:
            row = result.fetchone()
            if row is None:
                break
            yield row

    def __len__(self):
        result = self._execute_cached()
        return result.rowcount if hasattr(result, 'rowcount') else len(result.fetchall())
