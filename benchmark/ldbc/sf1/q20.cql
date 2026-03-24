MATCH
    (person1:Person {id: 17592186055119}),
    (person2:Person {id: 8796093025131}),
    path = shortestPath((person1)-[:KNOWS*]-(person2))
RETURN
    CASE path IS NULL
        WHEN true THEN -1
        ELSE length(path)
    END AS shortestPathLength