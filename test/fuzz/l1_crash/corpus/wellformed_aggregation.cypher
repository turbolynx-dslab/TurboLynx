MATCH (n:Person) RETURN count(n), avg(n.age), collect(n.name)
