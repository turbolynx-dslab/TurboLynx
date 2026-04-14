from .turbolynx_core import (
    # Connection
    connect,
    TurboLynxPyConnection,
    TurboLynxPyResult,
    PreparedStatement,

    # Module attributes
    apilevel,
    threadsafety,
    paramstyle,

    # DB-API 2.0 exceptions
    Error,
    DatabaseError,
    OperationalError,
    ProgrammingError,
    DataError,
    IntegrityError,
    InternalError,
    NotSupportedError,
)

# Re-export version
from .turbolynx_core import __version__

# Relation API
from .relation import TurboLynxRelation

# Graph API
from .graph import NodeQuery, PathQuery


def _connection_sql(self, query, params=None):
    """Create a lazy Relation from a Cypher query."""
    return TurboLynxRelation(self, query, _params=params or {})


def _connection_node(self, label, **props):
    """Create a graph node query builder.

    Example: conn.node("Person", firstName="Marc").neighbors("KNOWS").limit(5).fetchdf()
    """
    return NodeQuery(self, label, **props)


def _connection_shortest_path(self, source_label, target_label, rel=None,
                               min_hops=1, max_hops=None,
                               source_filter=None, target_filter=None,
                               direction="both"):
    """Find shortest path between two node types.

    Example: conn.shortest_path("Person", "Person", rel="KNOWS",
                                source_filter="a.firstName = 'Marc'",
                                target_filter="b.firstName = 'John'")
    """
    return PathQuery(self, source_label, target_label, rel=rel,
                     min_hops=min_hops, max_hops=max_hops, all_paths=False,
                     source_filter=source_filter, target_filter=target_filter,
                     direction=direction)


def _connection_all_shortest_paths(self, source_label, target_label, rel=None,
                                    min_hops=1, max_hops=None,
                                    source_filter=None, target_filter=None,
                                    direction="both"):
    """Find all shortest paths between two node types.

    Example: conn.all_shortest_paths("Person", "Person", rel="KNOWS",
                                     source_filter="a.id = 933",
                                     target_filter="b.id = 4139")
    """
    return PathQuery(self, source_label, target_label, rel=rel,
                     min_hops=min_hops, max_hops=max_hops, all_paths=True,
                     source_filter=source_filter, target_filter=target_filter,
                     direction=direction)


# Monkey-patch onto the C++ connection class
TurboLynxPyConnection.sql = _connection_sql
TurboLynxPyConnection.node = _connection_node
TurboLynxPyConnection.shortest_path = _connection_shortest_path
TurboLynxPyConnection.all_shortest_paths = _connection_all_shortest_paths


__all__ = [
    "connect",
    "TurboLynxPyConnection",
    "TurboLynxPyResult",
    "TurboLynxRelation",
    "NodeQuery",
    "PathQuery",
    "PreparedStatement",
    "__version__",
    "apilevel",
    "threadsafety",
    "paramstyle",
    # Exceptions
    "Error",
    "DatabaseError",
    "OperationalError",
    "ProgrammingError",
    "DataError",
    "IntegrityError",
    "InternalError",
    "NotSupportedError",
]
