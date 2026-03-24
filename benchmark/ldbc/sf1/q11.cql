MATCH (person:Person {id: 21990232559429 })-[:KNOWS]-(friend:Person),
      (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
WITH DISTINCT tag, post
WITH tag,
     CASE
       WHEN 1335830400000 <= post.creationDate < 1339027200000 THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN post.creationDate < 1335830400000 THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount>0 AND inValidPostCount=0
RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10