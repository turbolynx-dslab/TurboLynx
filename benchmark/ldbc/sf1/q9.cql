MATCH (n:Person {id: 17592186052613 })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
		WHERE message.creationDate <= 1354060800000
		RETURN
			friend.id AS personId,
			friend.firstName AS personFirstName,
			friend.lastName AS personLastName,
			message.id AS postOrCommentId,
        	coalesce(message.content,message.imageFile) AS postOrCommentContent,
			message.creationDate AS postOrCommentCreationDate
		ORDER BY
			postOrCommentCreationDate DESC,
			postOrCommentId ASC
		LIMIT 20;