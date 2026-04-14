"""Cypher query string manipulation for the Relation API.

Provides utilities to find and modify the RETURN clause of Cypher queries,
enabling lazy chaining operations like filter, project, order, limit.
"""

from collections import namedtuple

ReturnClauseInfo = namedtuple('ReturnClauseInfo', [
    'keyword_start',   # index of 'R' in RETURN
    'keyword_end',     # index after 'RETURN' (or 'RETURN DISTINCT')
    'columns_start',   # index where column expressions begin
    'columns_end',     # index where columns end (before ORDER BY etc.)
    'has_distinct',    # bool
    'suffix',          # the ORDER BY / SKIP / LIMIT text after columns
])


def _is_word_boundary(ch):
    """Check if a character is a word boundary (not alphanumeric or underscore)."""
    return not (ch.isalnum() or ch == '_')


def find_last_return(query):
    """Find the last top-level RETURN clause in a Cypher query.

    Uses a character-by-character scanner that tracks quote state to avoid
    matching RETURN inside string literals or comments.

    Returns ReturnClauseInfo or None if no RETURN found.
    """
    q = query
    n = len(q)
    i = 0
    last_return_pos = -1

    # Scanner state
    in_single_quote = False
    in_double_quote = False
    in_backtick = False
    in_line_comment = False

    while i < n:
        ch = q[i]

        # Line comment handling
        if in_line_comment:
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue

        # Quote state tracking
        if in_single_quote:
            if ch == '\\' and i + 1 < n:
                i += 2  # skip escaped char
                continue
            if ch == "'":
                in_single_quote = False
            i += 1
            continue

        if in_double_quote:
            if ch == '\\' and i + 1 < n:
                i += 2
                continue
            if ch == '"':
                in_double_quote = False
            i += 1
            continue

        if in_backtick:
            if ch == '`':
                in_backtick = False
            i += 1
            continue

        # Enter quote/comment states
        if ch == "'":
            in_single_quote = True
            i += 1
            continue
        if ch == '"':
            in_double_quote = True
            i += 1
            continue
        if ch == '`':
            in_backtick = True
            i += 1
            continue
        if ch == '/' and i + 1 < n and q[i + 1] == '/':
            in_line_comment = True
            i += 2
            continue

        # Check for RETURN keyword (case-insensitive)
        if ch in ('R', 'r') and i + 5 < n:
            word = q[i:i+6]
            if word.upper() == 'RETURN':
                # Check word boundaries
                before_ok = (i == 0 or _is_word_boundary(q[i - 1]))
                after_ok = (i + 6 >= n or _is_word_boundary(q[i + 6]))
                if before_ok and after_ok:
                    last_return_pos = i

        i += 1

    if last_return_pos < 0:
        return None

    # Parse the RETURN clause details
    ret_start = last_return_pos
    ret_kw_end = ret_start + 6  # after "RETURN"

    # Check for DISTINCT
    rest_after_return = q[ret_kw_end:].lstrip()
    has_distinct = rest_after_return[:8].upper() == 'DISTINCT' and (
        len(rest_after_return) <= 8 or _is_word_boundary(rest_after_return[8])
    )

    if has_distinct:
        # Find where DISTINCT actually is in the original string
        dist_offset = q[ret_kw_end:].upper().index('DISTINCT')
        columns_start = ret_kw_end + dist_offset + 8
    else:
        columns_start = ret_kw_end

    # Skip whitespace to get to actual column expressions
    while columns_start < n and q[columns_start] in (' ', '\t', '\n', '\r'):
        columns_start += 1

    # Find where columns end: scan for ORDER BY, SKIP, LIMIT, UNION at top level
    columns_end = n
    suffix_keywords = ['ORDER BY', 'SKIP', 'LIMIT', 'UNION']
    j = columns_start
    in_sq = False
    in_dq = False
    in_bt = False
    in_lc = False
    paren_depth = 0

    while j < n:
        ch = q[j]

        if in_lc:
            if ch == '\n':
                in_lc = False
            j += 1
            continue
        if in_sq:
            if ch == '\\' and j + 1 < n:
                j += 2
                continue
            if ch == "'":
                in_sq = False
            j += 1
            continue
        if in_dq:
            if ch == '\\' and j + 1 < n:
                j += 2
                continue
            if ch == '"':
                in_dq = False
            j += 1
            continue
        if in_bt:
            if ch == '`':
                in_bt = False
            j += 1
            continue

        if ch == "'":
            in_sq = True
            j += 1
            continue
        if ch == '"':
            in_dq = True
            j += 1
            continue
        if ch == '`':
            in_bt = True
            j += 1
            continue
        if ch == '/' and j + 1 < n and q[j + 1] == '/':
            in_lc = True
            j += 2
            continue
        if ch == '(':
            paren_depth += 1
            j += 1
            continue
        if ch == ')':
            paren_depth -= 1
            j += 1
            continue

        # Only match suffix keywords at top level (paren_depth == 0)
        if paren_depth == 0:
            for kw in suffix_keywords:
                kw_len = len(kw)
                if j + kw_len <= n and q[j:j+kw_len].upper() == kw:
                    before_ok = (j == 0 or _is_word_boundary(q[j - 1]))
                    after_ok = (j + kw_len >= n or _is_word_boundary(q[j + kw_len]))
                    if before_ok and after_ok:
                        columns_end = j
                        # Trim trailing whitespace from columns
                        while columns_end > columns_start and q[columns_end - 1] in (' ', '\t', '\n', '\r'):
                            columns_end -= 1
                        suffix = q[j:].strip()
                        return ReturnClauseInfo(
                            keyword_start=ret_start,
                            keyword_end=ret_kw_end,
                            columns_start=columns_start,
                            columns_end=columns_end,
                            has_distinct=has_distinct,
                            suffix=suffix,
                        )
        j += 1

    # No suffix keywords found — columns go to end of query
    columns_end = n
    while columns_end > columns_start and q[columns_end - 1] in (' ', '\t', '\n', '\r'):
        columns_end -= 1

    return ReturnClauseInfo(
        keyword_start=ret_start,
        keyword_end=ret_kw_end,
        columns_start=columns_start,
        columns_end=columns_end,
        has_distinct=has_distinct,
        suffix='',
    )


def split_columns(cols_str):
    """Split a comma-separated column list, respecting parens and quotes.

    Returns list of column expression strings (stripped).
    """
    result = []
    current = []
    paren_depth = 0
    in_sq = False
    in_dq = False
    in_bt = False

    for ch in cols_str:
        if in_sq:
            current.append(ch)
            if ch == "'":
                in_sq = False
            continue
        if in_dq:
            current.append(ch)
            if ch == '"':
                in_dq = False
            continue
        if in_bt:
            current.append(ch)
            if ch == '`':
                in_bt = False
            continue

        if ch == "'":
            in_sq = True
            current.append(ch)
        elif ch == '"':
            in_dq = True
            current.append(ch)
        elif ch == '`':
            in_bt = True
            current.append(ch)
        elif ch == '(':
            paren_depth += 1
            current.append(ch)
        elif ch == ')':
            paren_depth -= 1
            current.append(ch)
        elif ch == ',' and paren_depth == 0:
            result.append(''.join(current).strip())
            current = []
        else:
            current.append(ch)

    if current:
        result.append(''.join(current).strip())

    return [c for c in result if c]


def extract_alias(col_expr):
    """Extract the alias from a column expression like 'expr AS alias'.

    Returns (expr, alias). If no AS, alias is derived from the expression.
    """
    # Find AS keyword outside quotes/parens
    upper = col_expr.upper()
    paren_depth = 0
    in_sq = False
    in_dq = False
    in_bt = False
    i = 0

    while i < len(col_expr):
        ch = col_expr[i]
        if in_sq:
            if ch == "'":
                in_sq = False
            i += 1
            continue
        if in_dq:
            if ch == '"':
                in_dq = False
            i += 1
            continue
        if in_bt:
            if ch == '`':
                in_bt = False
            i += 1
            continue

        if ch == "'":
            in_sq = True
        elif ch == '"':
            in_dq = True
        elif ch == '`':
            in_bt = True
        elif ch == '(':
            paren_depth += 1
        elif ch == ')':
            paren_depth -= 1
        elif paren_depth == 0 and upper[i:i+3] == ' AS' and i + 3 < len(col_expr):
            after = col_expr[i + 3]
            if after in (' ', '\t', '`'):
                expr = col_expr[:i].strip()
                alias = col_expr[i+3:].strip().strip('`')
                return expr, alias

        i += 1

    # No AS found — use the expression itself as alias
    return col_expr.strip(), col_expr.strip()


def _strip_suffix(query, info):
    """Remove ORDER BY/SKIP/LIMIT/UNION suffix from a query, return (query_without_suffix, suffix)."""
    if info.suffix:
        # Find where suffix starts in original query
        suffix_start = info.columns_end
        # Skip whitespace between columns end and suffix
        q_stripped = query[:suffix_start].rstrip()
        return q_stripped, info.suffix
    return query.rstrip(), ''


def _parse_suffix(suffix):
    """Parse suffix string into components: order_by, skip, limit."""
    order_by = None
    skip = None
    limit = None

    upper = suffix.upper()
    # Find ORDER BY
    idx = upper.find('ORDER BY')
    if idx >= 0:
        # Find where ORDER BY value ends (at SKIP, LIMIT, or end)
        end = len(suffix)
        for kw in ['SKIP', 'LIMIT']:
            kw_idx = upper.find(kw, idx + 8)
            if kw_idx >= 0:
                end = min(end, kw_idx)
        order_by = suffix[idx + 8:end].strip()

    # Find SKIP
    idx = upper.find('SKIP')
    if idx >= 0:
        end = len(suffix)
        for kw in ['LIMIT']:
            kw_idx = upper.find(kw, idx + 4)
            if kw_idx >= 0:
                end = min(end, kw_idx)
        skip = suffix[idx + 4:end].strip()

    # Find LIMIT
    idx = upper.find('LIMIT')
    if idx >= 0:
        limit = suffix[idx + 5:].strip()

    return order_by, skip, limit


def _build_suffix(order_by=None, skip=None, limit=None):
    """Build suffix string from components."""
    parts = []
    if order_by:
        parts.append(f'ORDER BY {order_by}')
    if skip:
        parts.append(f'SKIP {skip}')
    if limit:
        parts.append(f'LIMIT {limit}')
    return ' '.join(parts)


def apply_filter(query, filter_expr):
    """Apply a WHERE filter by inserting WITH * WHERE ... before the last RETURN."""
    info = find_last_return(query)
    if info is None:
        raise ValueError("Cannot filter a query without RETURN clause")

    # Insert "WITH * WHERE <expr>" just before the last RETURN keyword
    prefix = query[:info.keyword_start].rstrip()
    rest = query[info.keyword_start:]  # "RETURN ..." with suffix

    return f'{prefix} WITH * WHERE {filter_expr} {rest}'


def apply_project(query, project_cols):
    """Replace the RETURN column list."""
    info = find_last_return(query)
    if info is None:
        raise ValueError("Cannot project on a query without RETURN clause")

    prefix = query[:info.columns_start]
    distinct = ''
    if info.has_distinct:
        # DISTINCT is already part of prefix (between RETURN and columns_start)
        pass
    result = prefix + project_cols
    if info.suffix:
        result += ' ' + info.suffix
    return result


def apply_order(query, order_expr):
    """Set or replace ORDER BY in the query suffix."""
    info = find_last_return(query)
    if info is None:
        raise ValueError("Cannot order a query without RETURN clause")

    base = query[:info.columns_end]
    if info.suffix:
        order_by, skip, limit = _parse_suffix(info.suffix)
        new_suffix = _build_suffix(order_by=order_expr, skip=skip, limit=limit)
    else:
        new_suffix = f'ORDER BY {order_expr}'

    return f'{base} {new_suffix}'


def apply_limit(query, limit_val):
    """Set or replace LIMIT in the query suffix."""
    info = find_last_return(query)
    if info is None:
        raise ValueError("Cannot limit a query without RETURN clause")

    base = query[:info.columns_end]
    if info.suffix:
        order_by, skip, limit = _parse_suffix(info.suffix)
        new_suffix = _build_suffix(order_by=order_by, skip=skip, limit=str(limit_val))
    else:
        new_suffix = f'LIMIT {limit_val}'

    return f'{base} {new_suffix}'


def apply_skip(query, skip_val):
    """Set or replace SKIP in the query suffix."""
    info = find_last_return(query)
    if info is None:
        raise ValueError("Cannot skip on a query without RETURN clause")

    base = query[:info.columns_end]
    if info.suffix:
        order_by, skip, limit = _parse_suffix(info.suffix)
        new_suffix = _build_suffix(order_by=order_by, skip=str(skip_val), limit=limit)
    else:
        new_suffix = f'SKIP {skip_val}'

    return f'{base} {new_suffix}'


def apply_distinct(query):
    """Add DISTINCT to the RETURN clause."""
    info = find_last_return(query)
    if info is None:
        raise ValueError("Cannot apply distinct on a query without RETURN clause")

    if info.has_distinct:
        return query  # already distinct

    # Insert DISTINCT after RETURN keyword
    return query[:info.keyword_end] + ' DISTINCT' + query[info.keyword_end:]


def apply_aggregate(query, aggr_exprs, group_by=None):
    """Replace RETURN with aggregation expressions."""
    if group_by:
        return apply_project(query, f'{group_by}, {aggr_exprs}')
    else:
        return apply_project(query, aggr_exprs)


def apply_count(query):
    """Replace RETURN columns with count expression.

    Uses the first column expression as the count argument to avoid
    ORCA optimizer issues with count(*).
    """
    info = find_last_return(query)
    if info is None:
        raise ValueError("Cannot count a query without RETURN clause")

    cols_str = query[info.columns_start:info.columns_end]
    columns = split_columns(cols_str)
    if columns:
        # Use first column expression for count
        expr, _ = extract_alias(columns[0])
        return apply_project(query, f'count({expr}) AS count_star')
    return apply_project(query, 'count(*) AS count_star')
