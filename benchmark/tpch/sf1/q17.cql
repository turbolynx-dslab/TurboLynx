MATCH (lineitem: LINEITEM)-[:COMPOSED_BY]->(part: PART)
WHERE part.P_BRAND = "Brand#15" AND part.P_CONTAINER = "LG CASE"
WITH lineitem, part
MATCH (lineitem2: LINEITEM)-[:COMPOSED_BY]->(part)
WITH lineitem, 0.2 * avg(lineitem2.L_QUANTITY) as avg_quantity
WHERE lineitem.L_QUANTITY < avg_quantity
RETURN sum(lineitem.L_EXTENDEDPRICE) / 7.0 as avg_yearly;