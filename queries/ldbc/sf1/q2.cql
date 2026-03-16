MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(message:Comment)
WITH
	message,
	message.id AS messageId,
	message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
MATCH (message)-[:REPLY_OF*0..]->(post:Post),
      (post)-[:HAS_CREATOR]->(person)
RETURN
	messageId,
	message.content AS messageContent,
	messageCreationDate,
	post.id AS postId,
	person.id AS personId,
	person.firstName AS personFirstName,
	person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC;