package dbtest.queryHandler.implementation;

import dbtest.queryHandler.AbstractQueryHandler;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.TypeHasNoValueException;
import dbtest.queryHandler.exceptions.QHException;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.neo4j.driver.v1.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

	/**
	 * No need for setup, since any structure in neo4j is created by inserting.
	 */
	@Override
	public void setUpDatabase()
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
	 */
	@Override
	public void storeJCasDocument(JCas document)
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
	}

	/**
	 * @param documents An iterable object of documents.
	 */
	@Override
	public void storeJCasDocuments(Iterable<JCas> documents)
	{
		for (JCas document : documents)
		{
			this.storeJCasDocument(document);
		}
	}

	/**
	 * @param paragraph         The Paragraph.
	 * @param document          The document in which the paragraph occurs.
	 * @param previousParagraph The predecessing Paragraph.
	 */
	@Override
	public void storeParagraph(
			Paragraph paragraph, JCas document, Paragraph previousParagraph
	)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		try (Session session = this.driver.session())
		{
			session.writeTransaction(tx -> {
				Map<String, Object> queryParams = new HashMap<>();

				// Create paragraph (if not exists) and add relationship from
				// document.
				String paragraphQuery = "MATCH (d:" + ElementType.Document + " {id:{documentId}}) ";
				queryParams.put("documentId", documentId);

				// Add successor relationship from previous paragraph (if
				// exists) to current paragraph.
				// Match has to be done before the first Merge, so this is split
				// into two parts.
				if (previousParagraph != null)
				{
					paragraphQuery += "MATCH (p_prev:" + ElementType.Paragraph + " {id:{documentId}, begin:{prevParagraphBegin}, end:{prevParagraphEnd}}) ";
					queryParams.put("prevParagraphBegin", previousParagraph.getBegin());
					queryParams.put("prevParagraphEnd", previousParagraph.getEnd());
				}

				paragraphQuery += "MERGE (p:" + ElementType.Paragraph + " {id:{documentId}, begin:{paragraphBegin}, end:{paragraphEnd}}) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasParagraph + "]->(p)";
				queryParams.put("paragraphBegin", paragraph.getBegin());
				queryParams.put("paragraphEnd", paragraph.getEnd());

				// Continue adding successor relationship.
				if (previousParagraph != null)
				{
					paragraphQuery += "MERGE (p_prev)-[:" + Relationship.NextParagraph + "]->(p)";
				}

				tx.run(paragraphQuery, queryParams);
				tx.success();
				return 1;
			});
		}
	}

	/**
	 * @param sentence         The Sentence.
	 * @param document         The Document in which the entence occurs.
	 * @param paragraph        The Paragraph, in which the Sentence occurs.
	 * @param previousSentence The predecessing Sentence.
	 */
	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph,
			Sentence previousSentence
	)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		try (Session session = this.driver.session())
		{
			session.writeTransaction(tx -> {
				Map<String, Object> queryParams = new HashMap<>();

				// Create sentence (if not exists) and add relationship from
				// document and to paragraph.
				String sentenceQuery = "MATCH (d:" + ElementType.Document + " {id:{documentId}}) "
						+ "MATCH (p:" + ElementType.Paragraph + " {id:{documentId}, begin:{paragraphBegin}, end:{paragraphEnd}}) ";
				queryParams.put("documentId", documentId);
				queryParams.put("paragraphBegin", paragraph.getBegin());
				queryParams.put("paragraphEnd", paragraph.getEnd());


				// Add successor relationship from previous sentence (if
				// exists) to current sentence.
				// Match has to be done before the first Merge, so this is split
				// into two parts.
				if (previousSentence != null)
				{
					sentenceQuery += "MATCH (s_prev:" + ElementType.Sentence + " {id:{documentId}, begin:{prevSentenceBegin}, end:{prevSentenceEnd}}) ";
					queryParams.put("prevSentenceBegin", previousSentence.getBegin());
					queryParams.put("prevSentenceEnd", previousSentence.getEnd());
				}

				sentenceQuery += "MERGE (s:" + ElementType.Sentence + " {id:{documentId}, begin:{sentenceBegin}, end:{sentenceEnd}}) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasSentence + "]->(s) "
						+ "MERGE (s)-[:" + Relationship.SentenceInParagraph + "]->(p) ";
				queryParams.put("sentenceBegin", sentence.getBegin());
				queryParams.put("sentenceEnd", sentence.getEnd());

				// Continue adding successor relationship.
				if (previousSentence != null)
				{
					sentenceQuery += "MERGE (s_prev)-[:" + Relationship.NextSentence + "]->(s)";
				}

				tx.run(sentenceQuery, queryParams);
				tx.success();
				return 1;
			});
		}
	}

	/**
	 * @param token         The Token.
	 * @param document      The id of the document in which the Token occurs.
	 * @param paragraph     The paragraph, in which the Token occurs.
	 * @param sentence      The sentence, in which the Token occurs.
	 * @param previousToken The predecessing Token.
	 */
	@Override
	public void storeToken(
			Token token,
			JCas document,
			Paragraph paragraph,
			Sentence sentence,
			Token previousToken
	)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		try (Session session = this.driver.session())
		{
			session.writeTransaction(tx -> {
				Map<String, Object> queryParams = new HashMap<>();

				// Create token (if not exists) and add relationship
				// from Document and to Paragraph and to Sentence.
				// Also create Lemma and Pos and add relationships from
				// Token to them as well as from Document to Lemma.
				String tokenQuery = "MATCH (d:" + ElementType.Document + " {id:{documentId}}) "
						+ "MATCH (p:" + ElementType.Paragraph + " {id:{documentId}, begin:{paragraphBegin}, end:{paragraphEnd}}) "
						+ "MATCH (s:" + ElementType.Sentence + " {id:{documentId}, begin:{sentenceBegin}, end:{sentenceEnd}}) ";
				queryParams.put("documentId", documentId);
				queryParams.put("paragraphBegin", paragraph.getBegin());
				queryParams.put("paragraphEnd", paragraph.getEnd());
				queryParams.put("sentenceBegin", sentence.getBegin());
				queryParams.put("sentenceEnd", sentence.getEnd());

				if (previousToken != null)
				{
					tokenQuery += "MATCH (t_prev:" + ElementType.Token + " {id:{documentId}, begin:{prevTokenBegin}, end:{prevTokenEnd}, value:{prevTokenValue}}) ";
					queryParams.put("prevTokenBegin", previousToken.getBegin());
					queryParams.put("prevTokenEnd", previousToken.getEnd());
					queryParams.put("prevTokenValue", previousToken.getCoveredText());
				}

				tokenQuery += "MERGE (t:" + ElementType.Token + " {id:{documentId}, begin:{tokenBegin}, end:{tokenEnd}, value:{tokenValue}}) "
						+ "MERGE (pos:" + ElementType.Pos + " {value:{tokenPosValue}}) "
						+ "MERGE (l:" + ElementType.Lemma + " {value:{tokenLemmaValue}}) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasToken + "]->(t) "
						+ "MERGE (t)-[:" + Relationship.TokenInParagraph + "]->(p) "
						+ "MERGE (t)-[:" + Relationship.TokenInSentence + "]->(s) "
						+ "MERGE (t)-[:" + Relationship.TokenHasLemma + "]->(l) "
						+ "MERGE (t)-[:" + Relationship.TokenAtPos + "]->(pos) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasLemma + "]->(l)";
				queryParams.put("tokenBegin", token.getBegin());
				queryParams.put("tokenEnd", token.getEnd());
				queryParams.put("tokenValue", token.getCoveredText());
				queryParams.put("tokenPosValue", token.getPos().getPosValue());
				queryParams.put("tokenLemmaValue", token.getLemma().getValue());

				if (previousToken != null)
				{
					tokenQuery += "MERGE (t_prev)-[:" + Relationship.NextToken + "]->(t)";
				}

				tx.run(tokenQuery, queryParams);
				tx.success();
				return 1;
			});
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
				ids.add(record.get("id").toString());
			}
		}
		return ids;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
	{
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
				lemmata.add(row.get("lemma").toString());
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
					meta.setDocumentId(document.get("id").toString());
					aCAS.setDocumentLanguage(document.get("language").toString());
					aCAS.setDocumentText(document.get("text").toString());
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
						StatementResult lemmasResult = tx.run("MATCH (t:" + ElementType.Token + " {id:{documentId}, begin:{tokenBegin}, end:{tokenEnd}, value:{tokenValue}}))-[:" + Relationship.TokenHasLemma + "]->(l:" + ElementType.Lemma + ") RETURN l as lemma", tokenParams);
						while (lemmasResult.hasNext())
						{
							Value foundLemma = lemmasResult.next().get("lemma");
							Lemma lemma = new Lemma(aCAS.getJCas(), xmiToken.getBegin(), xmiToken.getEnd());
							lemma.setValue(foundLemma.get("value").toString());
							lemma.addToIndexes();
							xmiToken.setLemma(lemma);
						}

						StatementResult posResult = tx.run("MATCH (t:" + ElementType.Token + " {id:{documentId}, begin:{tokenBegin}, end:{tokenEnd}, value:{tokenValue}}))-[:" + Relationship.TokenAtPos + "]->(pos:" + ElementType.Pos + ") RETURN pos", tokenParams);
						while (posResult.hasNext())
						{
							Value foundPos = posResult.next().get("pos");
							POS pos = new POS(aCAS.getJCas(), xmiToken.getBegin(), xmiToken.getEnd());
							pos.setPosValue(foundPos.get("value").toString());
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
					throw new DocumentNotFoundException();
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
					tx.run("MATCH (:" + ElementType.Token + ")-[:" + Relationship.TokenHasLemma + "]-(l:LEMMA) RETURN l.value AS lemma, count(l) AS count;")
			);

			while (result.hasNext())
			{
				Record row = result.next();
				lemmaOccurenceCount.put(
						row.get("lemma").toString(),
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
					tx -> tx.run("MATCH (d:" + ElementType.Document + ")--(t:" + ElementType.Token + ")--(l:" + ElementType.Lemma + ") WITH d, count(DISTINCT l)/count(t) AS ttr RETURN d.id AS id, ttr")
			);
			while (result.hasNext())
			{
				Record aRecord = result.next();
				documentTTRMap.put(aRecord.get("id").toString(), aRecord.get("ttr").asDouble());
			}
			return documentTTRMap;
		}
	}

	@Override
	public Double calculateTTRForDocument(String documentId)
			throws DocumentNotFoundException
	{
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
						return tx.run("MATCH (d:" + ElementType.Document + ")--(t:" + ElementType.Token + ")--(l:" + ElementType.Lemma + ") WHERE d.id in {documentIds} WITH d, count(DISTINCT l)/count(t) AS ttr RETURN d.id AS id, ttr", queryParams);
					}
			);
			while (result.hasNext())
			{
				Record aRecord = result.next();
				documentTTRMap.put(
						aRecord.get("id").toString(),
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
	protected Map<String, Integer> calculateRawTermFrequenciesInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		HashMap<String, Integer> rtf = new HashMap<>();
		try (Session session = this.driver.session())
		{
			Map<String, Object> queryParams = new HashMap<>();
			queryParams.put("documentId", documentId);
			StatementResult result = session.readTransaction(tx ->
					tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}) RETURN d", queryParams)
			);
			if (result == null || !result.hasNext())
			{
				throw new DocumentNotFoundException();
			}

			result = session.readTransaction(tx ->
					tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}})--(:" + ElementType.Token + ")--(l:" + ElementType.Lemma + ") WITH l, count(l.value) AS count RETURN l.value AS lemma, count;", queryParams)
			);

			if (result == null || !result.hasNext())
			{
				throw new DocumentNotFoundException();
			}

			while (result.hasNext())
			{
				Record row = result.next();
				rtf.put(row.get("lemma").toString().replaceAll("\"", ""), row.get("count").asInt());
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
	protected Integer calculateRawTermFrequencyForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException
	{
		try (Session session = this.driver.session())
		{
			Map<String, Object> documentParams = new HashMap<>();
			documentParams.put("documentId", documentId);
			StatementResult result = session.readTransaction(tx ->
					tx.run("MATCH (d:" + ElementType.Document + " {id:{documentId}) RETURN d", documentParams)
			);
			if (result == null || !result.hasNext())
			{
				throw new DocumentNotFoundException();
			}

			Map<String, Object> lemmaParams = new HashMap<>();
			lemmaParams.put("documentId", documentId);
			lemmaParams.put("lemmaValue", lemma);
			result = session.readTransaction(tx ->
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
	public double calculateTermFrequencyWithDoubleNormForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException
	{
		Map<String, Integer> rtf = this.calculateRawTermFrequenciesInDocument(
				documentId
		);
		return 0.5 + 0.5 * (
				((double) rtf.getOrDefault(lemma, 0)) /
						((double) Collections.max(rtf.values()))
		);
	}

	@Override
	public double calculateTermFrequencyWithLogNormForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException
	{
		double tf = 0;
		try (Session session = this.driver.session())
		{
			Integer lemmaCount =
					this.calculateRawTermFrequencyForLemmaInDocument(
							lemma,
							documentId
					);

			if (lemmaCount == 0)
			{
				return 1;
			} else
			{
				return 1 + Math.log(tf);
			}
		}
	}

	@Override
	public Map<String, Double> calculateTermFrequenciesForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		Map<String, Integer> rawTermFrequencies =
				this.calculateRawTermFrequenciesInDocument(documentId);
		Map<String, Double> termFrequencies = new ConcurrentHashMap<>();

		double max = Collections.max(rawTermFrequencies.values());
		rawTermFrequencies.entrySet().parallelStream().forEach(e -> {
			termFrequencies.put(
					e.getKey(),
					0.5 + 0.5 * (e.getValue() / max)
			);
		});
		return termFrequencies;
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(
			String documentId
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
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
				throw new DocumentNotFoundException();
			}

			while (result.hasNext())
			{
				Record row = result.next();
				biGrams.add(
						row.get("t1.value").toString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").toString()
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
						row.get("t1.value").toString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").toString()
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
				throw new DocumentNotFoundException();
			}

			while (result.hasNext())
			{
				Record row = result.next();
				biGrams.add(
						row.get("t1.value").toString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").toString()
								.replaceAll("\"", ""));
			}
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
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
				throw new DocumentNotFoundException();
			}

			while (result.hasNext())
			{
				Record row = result.next();
				triGrams.add(
						row.get("t1.value").toString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").toString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t3.value").toString()
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
						row.get("t1.value").toString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").toString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t3.value").toString()
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
				throw new DocumentNotFoundException();
			}

			while (result.hasNext())
			{
				Record row = result.next();
				triGrams.add(
						row.get("t1.value").toString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t2.value").toString()
								.replaceAll("\"", "")
								+ "-"
								+ row.get("t3.value").toString()
								.replaceAll("\"", "")
				);
			}
		}
		return triGrams;
	}
}
