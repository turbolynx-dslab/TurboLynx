// Reachable sensitive resources after a secret leak.
MATCH (s:Secret {name: 'public-api-token'})<-[:USES_SECRET]-(w:Workload)-[:RUNS_AS]->(entry:Identity)
MATCH (entry)-[:CAN_ASSUME*0..2]->(actor:Identity)-[:CAN_ACCESS]->(r:Resource)
WHERE r.sensitivity IN ['critical', 'high']
RETURN actor.name AS actor, r.name AS resource, r.kind AS kind
ORDER BY actor ASC, resource ASC;
