package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
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

			if (previousBigram != null)
			{
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
		Document document = this.database.getCollection("document").find(
				Filters.eq("_id", documentId)
		).first();

		if (document == null)
		{
			throw new DocumentNotFoundException();
		}
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		List<String> documentIds = new ArrayList<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("document").find().iterator())
		{
			while (cursor.hasNext())
			{
				Document document = cursor.next();
				documentIds.add((String) document.get("_id"));
			}
		}

		return documentIds;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
			throws DocumentNotFoundException
	{
		Set<String> lemmata = new HashSet<>();

		Document document = this.database.getCollection("document")
				.find(
						Filters.eq("_id", documentId)
				).first();

		if (document == null)
		{
			throw new DocumentNotFoundException();
		}

		List<Document> lemmaDocuments =
				(List<Document>) document.get("lemmata");
		for (Document lemmaDocument : lemmaDocuments)
		{
			lemmata.add(lemmaDocument.getString("value"));
		}

		return lemmata;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{
		Document document = this.database.getCollection("document")
				.find(
						Filters.eq("_id", documentId)
				).first();

		if (document == null)
		{
			throw new DocumentNotFoundException();
		}

		try
		{
			// Create Document CAS
			DocumentMetaData meta = DocumentMetaData.create(aCAS);
			meta.setDocumentId(documentId);
			aCAS.setDocumentText(document.getString("text"));
			aCAS.setDocumentLanguage(document.getString("language"));

			try (MongoCursor<Document> cursor = this.database
					.getCollection("token").find(
							Filters.eq("documentId", documentId)
					).iterator())
			{
				while (cursor.hasNext())
				{
					Document token = cursor.next();

					Token xmiToken = new Token(
							aCAS.getJCas(),
							token.getInteger("begin"),
							token.getInteger("end")
					);

					Lemma lemma = new Lemma(
							aCAS.getJCas(),
							xmiToken.getBegin(),
							xmiToken.getEnd()
					);
					lemma.setValue(token.getString("lemmaValue"));
					lemma.addToIndexes();
					xmiToken.setLemma(lemma);

					POS pos = new POS(
							aCAS.getJCas(),
							xmiToken.getBegin(),
							xmiToken.getEnd()
					);
					pos.setPosValue(token.getString("posValue"));
					pos.addToIndexes();
					xmiToken.setPos(pos);

					xmiToken.addToIndexes();
				}
			}
		} catch (CASException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		return (int) this.database.getCollection("document").count(
				Filters.elemMatch(
						"lemmata",
						Filters.eq("value", lemma)
				)
		);
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		String collectionName;
		switch (type)
		{
			case Document:
				collectionName = "document";
				break;
			case Paragraph:
				collectionName = "paragraph";
				break;
			case Sentence:
				collectionName = "sentence";
				break;
			case Token:
				collectionName = "token";
				break;
			case Lemma:
				collectionName = "lemma";
				break;
			case Pos:
				collectionName = "pos";
				break;
			default:
				throw new IllegalArgumentException();
		}
		return (int) this.database.getCollection(collectionName).count();
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId, ElementType type
	) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		if (type == ElementType.Document)
		{
			// There is obviously always one Document in a Document.
			return 1;
		}

		Document document = this.database.getCollection("document").find(
				Filters.eq("_id", documentId)
		).first();

		switch (type)
		{
			case Paragraph:
				return document.getInteger("paragraphCount");
			case Sentence:
				return document.getInteger("sentenceCount");
			case Token:
				return document.getInteger("tokenCount");
			case Lemma:
				return ((List<Document>) document.get("lemmata")).size();
			case Pos:
				return ((List<Document>) document.get("pos")).size();
			default:
				throw new IllegalArgumentException();
		}
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws IllegalArgumentException, TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		// => type is either Token, Lemma or POS.

		switch (type)
		{
			case Token:
				return (int) this.database.getCollection("token").count(
						Filters.eq("lemmaValue", value)
				);
			case Lemma:
				// Will return 0 or 1, since Lemmata values are unique.
				return (int) this.database.getCollection("lemma").count(
						Filters.eq("value", value)
				);
			case Pos:
				// Will return 0 or 1, since POS values are unique.
				return (int) this.database.getCollection("pos").count(
						Filters.eq("value", value)
				);
			default:
				throw new IllegalArgumentException();
		}
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException, TypeHasNoValueException
	{
		this.checkIfDocumentExists(documentId);
		this.checkTypeHasValueField(type);
		// => type is either Token, Lemma or POS.

		switch (type)
		{
			case Token:
				return (int) this.database.getCollection("token").count(
						Filters.and(
								Filters.eq("lemmaValue", value),
								Filters.eq("documentId", documentId)
						)
				);
			case Lemma:
				// Will return 0 or 1, since Lemmata values are unique.
				// Checks, if there is any document with the given ID that
				// has a lemma with the given value.
				return (int) this.database.getCollection("document").count(
						Filters.and(
								Filters.eq("_id", documentId),
								Filters.elemMatch(
										"lemmata",
										Filters.eq("value", value)
								)
						)
				);
			case Pos:
				// Will return 0 or 1, since POS values are unique.
				// Checks, if there is any document with the given ID that
				// has a POS with the given value.
				return (int) this.database.getCollection("document").count(
						Filters.and(
								Filters.eq("_id", documentId),
								Filters.elemMatch(
										"pos",
										Filters.eq("value", value)
								)
						)
				);
			default:
				throw new IllegalArgumentException();
		}
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		Map<String, Integer> occurrenceMap = new HashMap<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("lemma").find().iterator())
		{
			while (cursor.hasNext())
			{
				Document lemma = cursor.next();
				occurrenceMap.put(
						lemma.getString("value"),
						lemma.getInteger("count")
				);
			}
		}

		return occurrenceMap;
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		Map<String, Double> ttrMap = new HashMap<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("document").find().iterator())
		{
			while (cursor.hasNext())
			{
				Document document = cursor.next();
				Integer lemmaCount = ((List<Document>) document.get("lemmata"))
						.size();

				ttrMap.put(
						document.getString("_id"),
						(double) document.getInteger("tokenCount") /
								(double) lemmaCount
				);
			}
		}

		return ttrMap;
	}

	@Override
	public Double calculateTTRForDocument(String documentId)
			throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		return this.calculateTTRForCollectionOfDocuments(
				Arrays.asList(documentId)
		).get(documentId);
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(
			Collection<String> documentIds
	)
	{
		Map<String, Double> ttrMap = new HashMap<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("document").find(
						Filters.in("_id", documentIds)
				).iterator())
		{
			while (cursor.hasNext())
			{
				Document document = cursor.next();
				Integer lemmaCount = ((List<Document>) document.get("lemmata"))
						.size();

				ttrMap.put(
						document.getString("_id"),
						(double) document.getInteger("tokenCount") /
								(double) lemmaCount
				);
			}
		}

		return ttrMap;
	}

	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(String documentId) throws DocumentNotFoundException
	{
		Map<String, Integer> frequencyMap = new HashMap<>();

		Document document = this.database.getCollection("document").find(
				Filters.eq("_id", documentId)
		).first();

		if (document == null)
		{
			throw new DocumentNotFoundException();
		}

		List<Document> lemmaDocuments = (List<Document>) document
				.get("lemmata");

		for (Document lemmaDocument : lemmaDocuments)
		{
			frequencyMap.put(
					lemmaDocument.getString("value"),
					lemmaDocument.getInteger("count")
			);
		}

		return frequencyMap;
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		Document document = this.database.getCollection("document").find(
				Filters.and(
						Filters.eq("_id", documentId),
						Filters.elemMatch(
								"lemmata",
								Filters.eq("value", lemma)
						)
				)
		).first();

		if (document == null)
		{
			// If there was no exception thrown in the existence check above,
			// and our query returned no document, then the lemma did not occur
			// in the document.
			return 0;
		}

		List<Document> lemmaDocuments = (List<Document>) document
				.get("lemmata");

		for (Document lemmaDocument : lemmaDocuments)
		{
			if (lemmaDocument.getString("value").equals(lemma))
			{
				return lemmaDocument.getInteger("count");
			}
		}

		// This should not happen, but if it does, it is accurate.
		return 0;
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		List<String> biGrams = new ArrayList<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("biGram").find(
						Filters.eq("documentId", documentId)
				).iterator())
		{
			while (cursor.hasNext())
			{
				Document biGram = cursor.next();
				biGrams.add(
						String.format(
								"%s-%s",
								biGram.getString("firstValue"),
								biGram.getString("secondValue")
						)
				);
			}
		}

		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		List<String> biGrams = new ArrayList<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("biGram").find().iterator())
		{
			while (cursor.hasNext())
			{
				Document biGram = cursor.next();
				biGrams.add(
						String.format(
								"%s-%s",
								biGram.getString("firstValue"),
								biGram.getString("secondValue")
						)
				);
			}
		}

		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException
	{
		List<String> biGrams = new ArrayList<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("biGram").find(
						Filters.in("documentId", documentIds)
				).iterator())
		{
			while (cursor.hasNext())
			{
				Document biGram = cursor.next();
				biGrams.add(
						String.format(
								"%s-%s",
								biGram.getString("firstValue"),
								biGram.getString("secondValue")
						)
				);
			}
		}

		return biGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		List<String> triGrams = new ArrayList<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("triGram").find(
						Filters.eq("documentId", documentId)
				).iterator())
		{
			while (cursor.hasNext())
			{
				Document triGram = cursor.next();
				triGrams.add(
						String.format(
								"%s-%s-%s",
								triGram.getString("firstValue"),
								triGram.getString("secondValue"),
								triGram.getString("thirdValue")
						)
				);
			}
		}

		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		List<String> triGrams = new ArrayList<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("triGram").find().iterator())
		{
			while (cursor.hasNext())
			{
				Document triGram = cursor.next();
				triGrams.add(
						String.format(
								"%s-%s-%s",
								triGram.getString("firstValue"),
								triGram.getString("secondValue"),
								triGram.getString("thirdValue")
						)
				);
			}
		}

		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException
	{
		List<String> triGrams = new ArrayList<>();

		try (MongoCursor<Document> cursor = this.database
				.getCollection("triGram").find(
						Filters.in("documentId", documentIds)
				).iterator())
		{
			while (cursor.hasNext())
			{
				Document triGram = cursor.next();
				triGrams.add(
						String.format(
								"%s-%s-%s",
								triGram.getString("firstValue"),
								triGram.getString("secondValue"),
								triGram.getString("thirdValue")
						)
				);
			}
		}

		return triGrams;
	}

	/**
	 * @param hexString
	 * @return an ObjectId instance, if the given hexString is valid. Otherwise
	 * returns null.
	 */
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
