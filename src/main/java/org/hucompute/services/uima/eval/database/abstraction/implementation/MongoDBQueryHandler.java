package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.connection.Connections;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MongoDBQueryHandler extends AbstractQueryHandler
{
	protected MongoClient client;
	protected MongoDatabase database;

	public MongoDBQueryHandler(MongoClient mongoClient)
	{
		this.client = mongoClient;
	}

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.MongoDB;
	}

	/**
	 * MongoDB creates databases and collections automatically once data is
	 * stored in them. Thus there is no need to set up the database explicitly.
	 */
	@Override
	public void setUpDatabase()
	{

	}

	@Override
	public void openDatabase() throws IOException
	{
		this.database = this.client.getDatabase(
				System.getenv("MONGODB_DB")
		);
	}

	@Override
	public void clearDatabase()
	{
		this.database.getCollection("document").drop();
		this.database.getCollection("paragraph").drop();
		this.database.getCollection("sentence").drop();
		this.database.getCollection("token").drop();
		this.database.getCollection("lemma").drop();
		this.database.getCollection("pos").drop();
		this.database.getCollection("biGram").drop();
		this.database.getCollection("triGram").drop();
	}

	@Override
	public String storeJCasDocument(JCas document)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		Map<String, AtomicInteger> posCount = new HashMap<>();
		Map<String, AtomicInteger> lemmaCount = new HashMap<>();

		// Count all the elements in the document
		int paragraphCount = 0;
		int sentenceCount = 0;
		int tokenCount = 0;
		for (Paragraph paragraph
				: JCasUtil.select(document, Paragraph.class))
		{
			paragraphCount++;
			for (Sentence sentence : JCasUtil.selectCovered(
					document,
					Sentence.class, paragraph
			))
			{
				sentenceCount++;
				for (Token token : JCasUtil.selectCovered(
						document, Token.class, sentence
				))
				{
					tokenCount++;

					String lemmaValue = token.getLemma().getValue();
					lemmaCount.putIfAbsent(lemmaValue, new AtomicInteger(0));
					lemmaCount.get(lemmaValue).incrementAndGet();

					String posValue = token.getPos().getPosValue();
					posCount.putIfAbsent(posValue, new AtomicInteger(0));
					posCount.get(posValue).incrementAndGet();
				}
			}
		}

		List<Document> lemmaDocuments = new ArrayList<>();
		lemmaCount.forEach((value, count) -> {
			lemmaDocuments.add(
					new Document("value", value)
							.append("count", count)
			);
		});
		List<Document> posDocuments = new ArrayList<>();
		posCount.forEach((value, count) -> {
			posDocuments.add(
					new Document("value", value)
							.append("count", count)
			);
		});

		// Careful! The Document document's (lol) "_id" field is NOT a bson
		// ObjectId, but a plain string!
		Document bsonDocument = new Document("_id", documentId)
				.append("text", document.getDocumentText())
				.append("language", document.getDocumentLanguage())
				.append("paragraphCount", paragraphCount)
				.append("sentenceCount", sentenceCount)
				.append("tokenCount", tokenCount)
				.append("lemmata", lemmaDocuments)
				.append("pos", posDocuments);

		this.database.getCollection("document").insertOne(bsonDocument);

		return documentId;
	}

	@Override
	public String storeParagraph(
			Paragraph paragraph,
			String documentId,
			String previousParagraphId
	)
	{
		ObjectId paragraphId = ObjectId.get();

		Document bsonDocument = new Document("_id", paragraphId)
				.append("documentId", documentId)
				.append("begin", paragraph.getBegin())
				.append("end", paragraph.getEnd())
				// It does not matter, if this is null. Can be queried later on
				// either way.
				.append(
						"previousParagraph",
						objectIdOrNull(previousParagraphId)
				);

		this.database.getCollection("paragraph").insertOne(bsonDocument);

		return paragraphId.toHexString();
	}

	@Override
	public String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId,
			String previousSentenceId
	)
	{
		ObjectId sentenceId = ObjectId.get();

		Document bsonDocument = new Document("_id", sentenceId)
				.append("documentId", documentId)
				.append("begin", sentence.getBegin())
				.append("end", sentence.getEnd())
				// It does not matter, if this is null. Can be queried later on
				// either way.
				.append("previousSentence", objectIdOrNull(previousSentenceId))
				.append("paragraphId", objectIdOrNull(paragraphId));

		this.database.getCollection("sentence").insertOne(bsonDocument);

		return sentenceId.toHexString();
	}

	@Override
	public String storeToken(
			Token token,
			String documentId,
			String paragraphId,
			String sentenceId,
			String previousTokenId
	)
	{
		ObjectId tokenId = ObjectId.get();

		String lemmaValue = token.getLemma().getValue();
		String posValue = token.getPos().getPosValue();

		Document bsonDocument = new Document("_id", tokenId)
				.append("documentId", documentId)
				.append("begin", token.getBegin())
				.append("end", token.getEnd())
				.append("lemmaValue", lemmaValue)
				.append("posValue", posValue)
				// It does not matter, if this is null. Can be queried later on
				// either way.
				.append("previousToken", objectIdOrNull(previousTokenId))
				.append("paragraphId", objectIdOrNull(paragraphId))
				.append("sentenceId", objectIdOrNull(sentenceId));

		this.database.getCollection("token").insertOne(bsonDocument);

		this.database.getCollection("lemma").updateOne(
				Filters.eq("value", lemmaValue),
				Updates.inc("count", 1),
				new UpdateOptions().upsert(true)
		);

		this.database.getCollection("pos").updateOne(
				Filters.eq("value", posValue),
				Updates.inc("count", 1),
				new UpdateOptions().upsert(true)
		);

		if (previousTokenId != null)
		{
			// Get value of previous Token for insertion into bigram.
			Document previousToken = this.database.getCollection("token")
					.find(
							Filters.eq(
									"_id",
									new ObjectId(previousTokenId)
							)
					).first();

			// Insert bigram with previous and current Token.
			Document biGram = new Document("documentId", documentId)
					.append("firstValue", previousToken.get("lemmaValue"))
					.append("secondValue", lemmaValue);
			this.database.getCollection("biGram").insertOne(biGram);

			// Check if previous Token was already second value in a bigram.
			Document previousBigram = this.database.getCollection("biGram")
					.find(
							Filters.eq(
									"firstValue",
									previousToken.get("lemmaValue")
							)
					).first();

			if (previousBigram != null) {
				// If the previous Token was the second value in a trigram, it
				// and its previous Token can be used in a new trigram.
				Document triGram = new Document("documentId", documentId)
						.append("firstValue", previousBigram.get("firstValue"))
						.append("secondValue", previousToken.get("lemmaValue"))
						.append("thirdValue", lemmaValue);
				this.database.getCollection("triGram").insertOne(triGram);
			}
		}

		return tokenId.toHexString();
	}

	@Override
	public void checkIfDocumentExists(String documentId) throws DocumentNotFoundException
	{

	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		return null;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
	{
		return null;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{

	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		return 0;
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId, ElementType type
	) throws DocumentNotFoundException
	{
		return 0;
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws IllegalArgumentException
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException
	{
		return 0;
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		return null;
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		return null;
	}

	@Override
	public Double calculateTTRForDocument(String documentId)
			throws DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(
			Collection<String> documentIds
	)
	{
		return null;
	}

	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(String documentId) throws DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	protected static ObjectId objectIdOrNull(String hexString)
	{
		try
		{
			return new ObjectId(hexString);
		} catch (IllegalArgumentException e)
		{
			return null;
		}
	}
}
