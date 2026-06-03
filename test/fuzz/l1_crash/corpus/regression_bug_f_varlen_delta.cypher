MATCH (a:Person {name:'Keanu'})-[:KNOWS*1..2]->(b:Person) RETURN b.name
