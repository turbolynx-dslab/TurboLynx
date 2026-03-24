MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
WHERE tag.name = "BasketballPlayer" OR baseTagClass.name = "BasketballPlayer"
WITH collect(tag.id) as tags
MATCH (:Person {id: 17592186052613 })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
WHERE tag.id in tags
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
ORDER BY
    replyCount DESC,
    toInteger(personId) ASC
LIMIT 20