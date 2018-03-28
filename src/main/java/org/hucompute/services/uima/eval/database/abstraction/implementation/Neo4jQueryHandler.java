package org.hucompute.services.uima.eval.database.abstraction.implementation;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.neo4j.driver.v1.*;

import java.io.IOException;
import java.util.*;

public class Neo4jQueryHandler extends AbstractQueryHandler
{
	protected Driver driver;

	public enum Relationship
	{
		DocumentHasParagraph, DocumentHasSentence, DocumentHasToken,
		DocumentHasLemma,
		SentenceInParagraph, TokenInParagraph,
		TokenInSentence,
		TokenHasLemma, TokenAtPos,
		NextParagraph, NextSentence, NextToken
	}

	public Neo4jQueryHandler(Driver driver)
	{
		this.driver = driver;
	}

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.Neo4j;
	}

	/**
	 * No need for setup, since any structure in neo4j is created by inserting.
	 */
	@Override
	public void setUpDatabase()
	{
	}

	@Override
	public void openDatabase() throws IOException
	{

	}

	/**
	 * Clear content in chunks of 50k Nodes/Relationships.
	 */
	@Override
	public void clearDatabase()
	{
		try (Session session = this.driver.session())
		{
			session.writeTransaction(tx -> {
				StatementResult result;

				do
				{
					result = tx.run("MATCH ()-[r]-()\n" +
							"WITH r LIMIT 50000\n" +
							"DELETE r\n" +
							"RETURN count(r) as count");

				} while (result.next().get("count").asInt() > 0);

				do
				{
					result = tx.run("MATCH (n)\n" +
							"WITH n LIMIT 50000\n" +
							"DELETE n\n" +
							"RETURN count(n) as count");

				} while (result.next().get("count").asInt() > 0);

				return 1;
			});
		}
	}

	/**
	 * @param document The JCas document.
	 * @return The document's id.
	 */
	@Override
	public String storeJCasDocument(JCas document)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		try (Session session = this.driver.session())
		{
			session.writeTransaction(tx -> {
				String documentQuery = "MERGE (d:" + ElementType.Document + " {id:{documentId}}) SET d.text = {text}, d.language = {language}";
				Map<String, Object> params = new HashMap<>();
				params.put("documentId", documentId);
				params.put("text", document.getDocumentText());
				params.put("language", document.getDocumentLanguage());
				tx.run(documentQuery, params);
				tx.success();
				return 1;
			});
		}

		return documentId;
	}

	/**
	 * @param paragraph           The Paragraph.
	 * @param documentId          The id of the document in which the paragraph
	 *                            occurs.
	 * @param previousParagraphId The predecessing Paragraph's id.
	 * @return The Paragraph's id.
	 */
	@Override
	public String storeParagraph(
			Paragraph paragraph,
			String documentId,
			String previousParagraphId
	)
	{
		try (Session session = this.driver.session())
		{
			return session.writeTransaction(tx -> {
				String paragraphId = UUID.randomUUID().toString();
				Map<String, Object> queryParams = new HashMap<>();

				// Create paragraph (if not exists) and add relationship from
				// document.
				String paragraphQuery = "MATCH (d:" + ElementType.Document + " {id:{documentId}}) ";
				queryParams.put("documentId", documentId);

				// Add successor relationship from previous paragraph (if
				// exists) to current paragraph.
				// Match has to be done before the first Merge, so this is split
				// into two parts.
				if (previousParagraphId != null)
				{
					paragraphQuery += "MATCH (p_prev:" + ElementType.Paragraph + " {paragraphId:{previousParagraphId}}) ";
					queryParams.put("previousParagraphId", previousParagraphId);
				}

				paragraphQuery += "MERGE (p:" + ElementType.Paragraph + " {paragraphId:{paragraphId}, id:{documentId}, begin:{paragraphBegin}, end:{paragraphEnd}}) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasParagraph + "]->(p)";
				queryParams.put("paragraphId", paragraphId);
				queryParams.put("paragraphBegin", paragraph.getBegin());
				queryParams.put("paragraphEnd", paragraph.getEnd());

				// Continue adding successor relationship.
				if (previousParagraphId != null)
				{
					paragraphQuery += "MERGE (p_prev)-[:" + Relationship.NextParagraph + "]->(p)";
				}

				tx.run(paragraphQuery, queryParams);
				tx.success();
				return paragraphId;
			});
		}
	}

	/**
	 * @param sentence           The Sentence.
	 * @param documentId         The id of the document in which the paragraph
	 *                           occurs.
	 * @param paragraphId        The id of the Paragraph in which the Sentence
	 *                           occurs.
	 * @param previousSentenceId The predecessing Sentence's id.
	 * @return The Sentence's id.
	 */
	@Override
	public String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId,
			String previousSentenceId
	)
	{
		try (Session session = this.driver.session())
		{
			return session.writeTransaction(tx -> {
				String sentenceId = UUID.randomUUID().toString();
				Map<String, Object> queryParams = new HashMap<>();

				// Create sentence (if not exists) and add relationship from
				// document and to paragraph.
				String sentenceQuery = "MATCH (d:" + ElementType.Document + " {id:{documentId}}) "
						+ "MATCH (p:" + ElementType.Paragraph + " {paragraphId:{paragraphId}}) ";
				queryParams.put("documentId", documentId);
				queryParams.put("paragraphId", paragraphId);


				// Add successor relationship from previous sentence (if
				// exists) to current sentence.
				// Match has to be done before the first Merge, so this is split
				// into two parts.
				if (previousSentenceId != null)
				{
					sentenceQuery += "MATCH (s_prev:" + ElementType.Sentence + " {sentenceId:{previousSentenceId}}) ";
					queryParams.put("previousSentenceId", previousSentenceId);
				}

				sentenceQuery += "MERGE (s:" + ElementType.Sentence + " {sentenceId:{sentenceId}, id:{documentId}, begin:{sentenceBegin}, end:{sentenceEnd}}) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasSentence + "]->(s) "
						+ "MERGE (s)-[:" + Relationship.SentenceInParagraph + "]->(p) ";
				queryParams.put("sentenceId", sentenceId);
				queryParams.put("sentenceBegin", sentence.getBegin());
				queryParams.put("sentenceEnd", sentence.getEnd());

				// Continue adding successor relationship.
				if (previousSentenceId != null)
				{
					sentenceQuery += "MERGE (s_prev)-[:" + Relationship.NextSentence + "]->(s)";
				}

				tx.run(sentenceQuery, queryParams);
				tx.success();
				return sentenceId;
			});
		}
	}

	/**
	 * @param token           The Token.
	 * @param documentId      The id of the document in which the paragraph
	 *                        occurs.
	 * @param paragraphId     The id of the Paragraph in which the Sentence
	 *                        occurs.
	 * @param sentenceId      The id of the Sentence in which the Token occurs.
	 * @param previousTokenId The predecessing Token's id.
	 * @return The Token's id.
	 */
	@Override
	public String storeToken(
			Token token,
			String documentId,
			String paragraphId,
			String sentenceId,
			String previousTokenId
	)
	{
		try (Session session = this.driver.session())
		{
			return session.writeTransaction(tx -> {
				String tokenId = UUID.randomUUID().toString();
				Map<String, Object> queryParams = new HashMap<>();

				// Create token (if not exists) and add relationship
				// from Document and to Paragraph and to Sentence.
				// Also create Lemma and Pos and add relationships from
				// Token to them as well as from Document to Lemma.
				String tokenQuery = "MATCH (d:" + ElementType.Document + " {id:{documentId}}) "
						+ "MATCH (p:" + ElementType.Paragraph + " {paragraphId:{paragraphId}}) "
						+ "MATCH (s:" + ElementType.Sentence + " {sentenceId:{sentenceId}}) ";
				queryParams.put("documentId", documentId);
				queryParams.put("paragraphId", paragraphId);
				queryParams.put("sentenceId", sentenceId);

				if (previousTokenId != null)
				{
					tokenQuery += "MATCH (t_prev:" + ElementType.Token + " {tokenId:{previousTokenId}}) ";
					queryParams.put("previousTokenId", previousTokenId);
				}

				tokenQuery += "MERGE (t:" + ElementType.Token + " {tokenId:{tokenId}, id:{documentId}, begin:{tokenBegin}, end:{tokenEnd}, value:{tokenValue}}) "
						+ "MERGE (pos:" + ElementType.Pos + " {value:{tokenPosValue}}) "
						+ "MERGE (l:" + ElementType.Lemma + " {value:{tokenLemmaValue}}) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasToken + "]->(t) "
						+ "MERGE (t)-[:" + Relationship.TokenInParagraph + "]->(p) "
						+ "MERGE (t)-[:" + Relationship.TokenInSentence + "]->(s) "
						+ "MERGE (t)-[:" + Relationship.TokenHasLemma + "]->(l) "
						+ "MERGE (t)-[:" + Relationship.TokenAtPos + "]->(pos) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasLemma + "]->(l)";
				queryParams.put("tokenId", tokenId);
				queryParams.put("tokenBegin", token.getBegin());
				queryParams.put("tokenEnd", token.getEnd());
				queryParams.put("tokenValue", token.getCoveredText());
				queryParams.put("tokenPosValue", token.getPos().getPosValue());
				queryParams.put("tokenLemmaValue", token.getLemma().getValue());

				if (previousTokenId != null)
				{
					tokenQuery += "MERGE (t_prev)-[:" + Relationship.NextToken + "]->(t)";
				}

				tx.run(tokenQuery, queryParams);
				tx.success();
				return tokenId;
			});
		}
	}

	@Override
	public void checkIfDocumentExists(String documentId)
			throws DocumentNotFoundException
	{
		try (Session session = this.driver.session())
		{
			DocumentNotFoundException e = session.readTransaction(tx -> {
				Map<String, Object> documentParams = new HashMap<>();
				documentParams.put("documentId", documentId);
				StatementResult documentResult = tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}}) RETURN d as document", documentParams);
				if (!documentResult.hasNext())
				{
					tx.failure();
					return new DocumentNotFoundException();
				}
				return null;
			});
			if (e != null)
			{
				throw e;
			}
		}
	}

	/**
	 * @return The ids of all Documents stored in the database.
	 */
	@Override
	public Iterable<String> getDocumentIds()
	{
		ArrayList<String> ids = new ArrayList<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(tx -> {
				return tx.run("MATCH (d:" + ElementType.Document + ") RETURN d.id as id");
			});
			for (Record record : result.list())
			{
				ids.add(record.get("id").asString());
			}
		}
		return ids;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
			throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		try (Session session = this.driver.session())
		{
			Map<String, Object> queryParams = new HashMap<>();
			Set<String> lemmata = new HashSet<>();
			String query = "MATCH (d:" + ElementType.Document + " {id:{documentId}})-[:" + Relationship.DocumentHasLemma + "]-(l:" + ElementType.Lemma + ") RETURN l.value AS lemma";
			queryParams.put("documentId", documentId);
			StatementResult result = session.readTransaction(tx -> tx.run(query, queryParams));

			while (result.hasNext())
			{
				Record row = result.next();
				lemmata.add(row.get("lemma").asString());
			}
			return lemmata;
		}
	}

	/**
	 * @param aCAS       The CAS to populate with the found data.
	 * @param documentId The document whose data shall be used.
	 * @throws DocumentNotFoundException If the documentId can't be found in db.
	 * @throws QHException               If any underlying Exception is thrown.
	 */
	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{
		try (Session session = this.driver.session())
		{
			Exception anException = session.readTransaction(tx -> {
				try
				{
					Map<String, Object> documentParams = new HashMap<>();
					documentParams.put("documentId", documentId);
					StatementResult documentResult = tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}}) RETURN d as document", documentParams);
					if (!documentResult.hasNext())
					{
						tx.failure();
						return new DocumentNotFoundException();
					}
					Value document = documentResult.next().get("document");

					DocumentMetaData meta = DocumentMetaData.create(aCAS);
					meta.setDocumentId(document.get("id").asString());
					aCAS.setDocumentLanguage(document.get("language").asString());
					aCAS.setDocumentText(document.get("text").asString());
					StatementResult tokensResult = tx.run("MATCH (t:" + ElementType.Token + " {id:{documentId}}) RETURN t as token", documentParams);
					while (tokensResult.hasNext())
					{
						Value foundToken = tokensResult.next().get("token");

						Token xmiToken = new Token(aCAS.getJCas(), foundToken.get("begin").asInt(), foundToken.get("end").asInt());

						Map<String, Object> tokenParams = new HashMap<>();
						tokenParams.put("documentId", documentId);
						tokenParams.put("tokenBegin", foundToken.get("begin").asInt());
						tokenParams.put("tokenEnd", foundToken.get("end").asInt());
						tokenParams.put("tokenValue", foundToken.get("value").asString());
						StatementResult lemmasResult = tx.run("MATCH (t:" + ElementType.Token + " {id:{documentId}, begin:{tokenBegin}, end:{tokenEnd}, value:{tokenValue}})-[:" + Relationship.TokenHasLemma + "]->(l:" + ElementType.Lemma + ") RETURN l as lemma", tokenParams);
						while (lemmasResult.hasNext())
						{
							Value foundLemma = lemmasResult.next().get("lemma");
							Lemma lemma = new Lemma(aCAS.getJCas(), xmiToken.getBegin(), xmiToken.getEnd());
							lemma.setValue(foundLemma.get("value").asString());
							lemma.addToIndexes();
							xmiToken.setLemma(lemma);
						}

						StatementResult posResult = tx.run("MATCH (t:" + ElementType.Token + " {id:{documentId}, begin:{tokenBegin}, end:{tokenEnd}, value:{tokenValue}})-[:" + Relationship.TokenAtPos + "]->(pos:" + ElementType.Pos + ") RETURN pos", tokenParams);
						while (posResult.hasNext())
						{
							Value foundPos = posResult.next().get("pos");
							POS pos = new POS(aCAS.getJCas(), xmiToken.getBegin(), xmiToken.getEnd());
							pos.setPosValue(foundPos.get("value").asString());
							pos.addToIndexes();
							xmiToken.setPos(pos);
						}

						xmiToken.addToIndexes();
					}
					tx.success();
				} catch (CASException e)
				{
					tx.failure();
					// If something happens, return the exception...
					return e;
				}
				return null;
			});

			// ... and throw the Exception out here.
			if (anException != null)
			{
				if (anException instanceof DocumentNotFoundException)
				{
					throw (DocumentNotFoundException) anException;
				}
				throw new QHException(anException);
			}
		}
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> {
						Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("lemmaValue", lemma);
						return tx.run("MATCH (d:" + ElementType.Document + ")-[:" + Relationship.DocumentHasLemma + "]->(l:" + ElementType.Lemma + "{value:{lemmaValue}}) RETURN count(d) as amount", queryParams);
					}
			);
			return result.next().get("amount").asInt();
		}
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> tx.run("MATCH (e:" + type + ") RETURN count(e) as amount")
			);
			return result.next().get("amount").asInt();
		}
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId,
			ElementType type
	) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> {
						Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("documentId", documentId);
						return tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}})--(e:" + type + ") RETURN count(e) as amount", queryParams);
					}
			);
			return result.next().get("amount").asInt();
		}
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> {
						Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("value", value);
						return tx.run("MATCH (e:" + type + " {value:{value}}) RETURN count(e) as amount", queryParams);
					}
			);
			return result.next().get("amount").asInt();
		}
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId,
			ElementType type,
			String value
	) throws DocumentNotFoundException, TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		this.checkIfDocumentExists(documentId);
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> {
						Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("documentId", documentId);
						queryParams.put("value", value);
						return tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}})--(e:" + type + " {value:{value}}) RETURN count(e) as amount", queryParams);
					}
			);
			return result.next().get("amount").asInt();
		}
	}

	/**
	 * Since every Token is connected to exactly one Document (the one it is
	 * contained in), we do not need to query the Documents and can instead
	 * count, in how many Tokens a Lemma occurs.
	 * <p>
	 * This can only be a problem, if there are dangling Tokens (which do not
	 * have a connection to a Document). However, this should never happen.
	 *
	 * @return The amount of occurences of each Lemma an all Documents.
	 */
	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		HashMap<String, Integer> lemmaOccurenceCount = new HashMap<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(tx ->
					tx.run("MATCH (l:" + ElementType.Lemma + ") RETURN l.value AS lemma, size((l)-[:" + Relationship.TokenHasLemma + "]-(:" + ElementType.Token + ")) AS count")
			);

			while (result.hasNext())
			{
				Record row = result.next();
				lemmaOccurenceCount.put(
						row.get("lemma").asString(),
						row.get("count").asInt()
				);
			}
		}
		return lemmaOccurenceCount;
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		HashMap<String, Double> documentTTRMap = new HashMap<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> tx.run("MATCH (d:" + ElementType.Document + ")--(t:" + ElementType.Token + ")--(l:" + ElementType.Lemma + ") WITH d.id AS id, count(DISTINCT l) AS cl, count(t) AS ct RETURN id, (cl / toFloat(ct)) AS ttr")
			);
			while (result.hasNext())
			{
				Record aRecord = result.next();
				documentTTRMap.put(aRecord.get("id").asString(), aRecord.get("ttr").asDouble());
			}
			return documentTTRMap;
		}
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
		HashMap<String, Double> documentTTRMap = new HashMap<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> {
						Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("documentIds", documentIds);
						return tx.run("MATCH (d:" + ElementType.Document + ")-->(t:" + ElementType.Token + ")-->(l:" + ElementType.Lemma + ") WHERE d.id in {documentIds} WITH d.id AS id, count(DISTINCT l) as cl, count(t) AS ct RETURN id, (cl / toFloat(ct)) AS ttr", queryParams);
					}
			);
			while (result.hasNext())
			{
				Record aRecord = result.next();
				documentTTRMap.put(
						aRecord.get("id").asString(),
						aRecord.get("ttr").asDouble()
				);
			}
			return documentTTRMap;
		}
	}

	/**
	 * Computes the term frequency without norming for each lemma in the speci-
	 * fied document.
	 *
	 * @param documentId The document to calculate frequencies for.
	 * @return a map from lemma to frequency
	 */
	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		HashMap<String, Integer> rtf = new HashMap<>();
		try (Session session = this.driver.session())
		{
			Map<String, Object> queryParams = new HashMap<>();
			queryParams.put("documentId", documentId);

			StatementResult result = session.readTransaction(tx ->
					tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}})--(:" + ElementType.Token + ")--(l:" + ElementType.Lemma + ") WITH l, count(l.value) AS count RETURN l.value AS lemma, count;", queryParams)
			);

			if (result == null || !result.hasNext())
			{
				return rtf;
			}

			while (result.hasNext())
			{
				Record row = result.next();
				rtf.put(row.get("lemma").asString().replaceAll("\"", ""), row.get("count").asInt());
			}
		}
		return rtf;
	}

	/**
	 * Computes the term frequency without norming for the given lemma in the
	 * specified document.
	 *
	 * @param lemma      The lemma to search for.
	 * @param documentId The document to calculate frequencies for.
	 * @return the lemma's term frequency
	 */
	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		try (Session session = this.driver.session())
		{
			Map<String, Object> lemmaParams = new HashMap<>();
			lemmaParams.put("documentId", documentId);
			lemmaParams.put("lemmaValue", lemma);
			StatementResult result = session.readTransaction(tx ->
					tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}})-[:" + Relationship.DocumentHasToken + "]-(:" + ElementType.Token + ")-[:" + Relationship.TokenHasLemma + "]-(l:" + ElementType.Lemma + " {value:{lemmaValue}}) WITH count(l.value) AS count RETURN count;", lemmaParams)
			);

			if (!result.hasNext())
			{
				return 0;
			} else
			{
				return result.next().get("count").asInt();
			}
		}
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(
			String documentId
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);

		ArrayList<String> biGrams = new ArrayList<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> {
						Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("documentId", documentId);
						return tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}})--(t1:" + ElementType.Token + ")-[:" + Relationship.NextToken + "]->(t2:" + ElementType.Token + ") RETURN t1.value, t2.value", queryParams);
					}
			);

			if (result == null || !result.hasNext())
			{
				return biGrams;
			}

			while (result.hasNext())
			{
				Record row = result.next();
				biGrams.add(
						row.get("t1.value").asString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").asString()
								.replaceAll("\"", ""));
			}
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		ArrayList<String> biGrams = new ArrayList<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(tx ->
					tx.run("MATCH (d:" + ElementType.Document + ")--(t1:" + ElementType.Token + ")-[:" + Relationship.NextToken + "]->(t2:" + ElementType.Token + ") RETURN t1.value, t2.value")
			);

			while (result.hasNext())
			{
				Record row = result.next();
				biGrams.add(
						row.get("t1.value").asString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").asString()
								.replaceAll("\"", ""));
			}
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		int documentsFound = 0;
		for (String documentId : documentIds)
		{
			try
			{
				this.checkIfDocumentExists(documentId);
				documentsFound++;
			} catch (DocumentNotFoundException ignored)
			{
			}
		}
		if (documentsFound == 0)
		{
			throw new DocumentNotFoundException();
		}

		ArrayList<String> biGrams = new ArrayList<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> {
						Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("documentIds", documentIds);
						return tx.run("MATCH (d:" + ElementType.Document + ")--(t1:" + ElementType.Token + ")-[:" + Relationship.NextToken + "]->(t2:" + ElementType.Token + ") WHERE d.id in {documentIds} RETURN t1.value, t2.value", queryParams);
					}
			);

			if (result == null || !result.hasNext())
			{
				return biGrams;
			}

			while (result.hasNext())
			{
				Record row = result.next();
				biGrams.add(
						row.get("t1.value").asString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").asString()
								.replaceAll("\"", ""));
			}
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		ArrayList<String> triGrams = new ArrayList<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> {
						Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("documentId", documentId);
						return tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}})--(t1:" + ElementType.Token + ")-[:" + Relationship.NextToken + "]->(t2:" + ElementType.Token + ")-[:" + Relationship.NextToken + "]->(t3:" + ElementType.Token + ") RETURN t1.value, t2.value, t3.value", queryParams);
					}
			);

			if (result == null || !result.hasNext())
			{
				// Probably no Tokens in the document.
				return triGrams;
			}

			while (result.hasNext())
			{
				Record row = result.next();
				triGrams.add(
						row.get("t1.value").asString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").asString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t3.value").asString()
								.replaceAll("\"", "")
				);
			}
		}
		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		ArrayList<String> triGrams = new ArrayList<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(tx ->
					tx.run("MATCH (d:" + ElementType.Document + ")--(t1:" + ElementType.Token + ")-[:" + Relationship.NextToken + "]->(t2:" + ElementType.Token + ")-[:" + Relationship.NextToken + "]->(t3:" + ElementType.Token + ") RETURN t1.value, t2.value, t3.value")
			);

			while (result.hasNext())
			{
				Record row = result.next();
				triGrams.add(
						row.get("t1.value").asString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").asString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t3.value").asString()
								.replaceAll("\"", "")
				);
			}
		}
		return triGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		int documentsFound = 0;
		for (String documentId : documentIds)
		{
			try
			{
				this.checkIfDocumentExists(documentId);
				documentsFound++;
			} catch (DocumentNotFoundException ignored)
			{
			}
		}
		if (documentsFound == 0)
		{
			throw new DocumentNotFoundException();
		}

		ArrayList<String> triGrams = new ArrayList<>();
		try (Session session = this.driver.session())
		{
			StatementResult result = session.readTransaction(
					tx -> {
						Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("documentIds", documentIds);
						return tx.run("MATCH (d:" + ElementType.Document + ")--(t1:" + ElementType.Token + ")-[:" + Relationship.NextToken + "]->(t2:" + ElementType.Token + ")-[:" + Relationship.NextToken + "]->(t3:" + ElementType.Token + ") WHERE d.id in {documentIds} RETURN t1.value, t2.value, t3.value", queryParams);
					}
			);

			if (result == null || !result.hasNext())
			{
				return triGrams;
			}

			while (result.hasNext())
			{
				Record row = result.next();
				triGrams.add(
						row.get("t1.value").asString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").asString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t3.value").asString()
								.replaceAll("\"", "")
				);
			}
		}
		return triGrams;
	}
}
