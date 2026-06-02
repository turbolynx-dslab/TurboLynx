MATCH p = shortestPath((a:Person {id:1})-[*]-(b:Person {id:2})) RETURN length(p)
