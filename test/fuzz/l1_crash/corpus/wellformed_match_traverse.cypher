MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name
