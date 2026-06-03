MATCH (n:Person) WITH n.age AS age WHERE age > 18 RETURN avg(age)
