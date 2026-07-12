MATCH (li:LINEITEM)
WHERE li.L_SHIPDATE >= date("1995-11-01") AND  li.L_SHIPDATE < date("1996-02-01")
WITH li.L_SUPPKEY as suppkey,  sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as total_revenue
WITH round(max(total_revenue),4) as max_total_revenue
MATCH (li2:LINEITEM)
WHERE li2.L_SHIPDATE >= date("1995-11-01") AND  li2.L_SHIPDATE < date("1996-02-01")
WITH max_total_revenue, li2.L_SUPPKEY as suppkey2, sum(li2.L_EXTENDEDPRICE * (1-li2.L_DISCOUNT)) as total_revenue
WHERE round(total_revenue,4) = max_total_revenue
MATCH (s2:SUPPLIER)
WHERE s2.S_SUPPKEY = suppkey2
RETURN s2.S_SUPPKEY, s2.S_NAME, s2.S_ADDRESS, s2.S_PHONE, total_revenue
ORDER BY s2.S_SUPPKEY;
