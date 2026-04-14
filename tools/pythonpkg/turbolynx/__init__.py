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


def _connection_sql(self, query, params=None):
    """Create a lazy Relation from a Cypher query."""
    return TurboLynxRelation(self, query, _params=params or {})


# Monkey-patch sql() onto the C++ connection class
TurboLynxPyConnection.sql = _connection_sql


__all__ = [
    "connect",
    "TurboLynxPyConnection",
    "TurboLynxPyResult",
    "TurboLynxRelation",
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
