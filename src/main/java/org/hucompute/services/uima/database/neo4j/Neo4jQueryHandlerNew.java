package org.hucompute.services.uima.database.neo4j;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.database.AbstractQueryHandler;
import org.hucompute.services.uima.database.Const;
import org.json.JSONObject;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Neo4jQueryHandlerNew extends AbstractQueryHandler
{
	protected Driver driver;

	public enum Label
	{
		Document, Paragraph, Sentence, Token, Lemma, Pos
	}

	public enum Relationship
	{
		DocumentHasParagraph, DocumentHasSentence, DocumentHasToken,
		DocumentHasLemma,
		SentenceInParagraph, TokenInParagraph,
		TokenInSentence,
		TokenHasLemma, TokenAtPos,
		NextParagraph, NextSentence, NextToken
	}

	public Neo4jQueryHandlerNew(Driver driver)
	{
		this.driver = driver;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
	{
		Set<String> lemmata = new HashSet<>();
		Session session = this.driver.session();
		String query = "MATCH (d:" + Label.Document + " {id:'" + documentId + "'})-[:" + Relationship.DocumentHasLemma + "]-(l:" + Label.Lemma + ") RETURN l.value AS lemma";
		StatementResult result = session.readTransaction(tx -> tx.run(query));

		while (result.hasNext())
		{
			Record row = result.next();
			lemmata.add(row.get("lemma").toString());
		}
		return lemmata;
	}

	/**
	 *
	 * @param document The JCas document.
	 */
	@Override
	public void storeJCasDocument(JCas document)
	{
		/* Document creation. */
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		Session session = this.driver.session();
		session.writeTransaction(tx -> {
			String documentQuery = "MERGE (d:" + Label.Document + " {id:'" + documentId + "'}) SET d.text = '" + document.getDocumentText() + "', d.language = '" + document.getDocumentLanguage() + "'";
			tx.run(documentQuery);

			/*
			 * Iterate over each element of the jCas that was annotated as a Pa-
			 * ragraph. Each paragraph has properties for its beginning and en-
			 * ding character position and the id of the document it is con-
			 * tained in. This is done so identical paragraphs don't have rela-
			 * tionships with more than one document.
			 */
			Paragraph previousParagraph = null;
			for (Paragraph paragraph
					: JCasUtil.select(document, Paragraph.class))
			{
				// Create paragraph (if not exists) and add relationship from
				// document.
				String paragraphQuery = "MATCH (d:" + Label.Document + " {id:'" + documentId + "'}) ";

				// Add successor relationship from previous paragraph (if
				// exists) to current paragraph.
				// Match has to be done before the first Merge, so this is split
				// into two parts.
				if (previousParagraph != null)
				{
					paragraphQuery += "MATCH (p_prev:" + Label.Paragraph + " {id:'" + documentId + "', begin:'" + previousParagraph.getBegin() + "', end:'" + previousParagraph.getEnd() + "'}) ";
				}

				paragraphQuery += "MERGE (p:" + Label.Paragraph + " {id:'" + documentId + "', begin:'" + paragraph.getBegin() + "', end:'" + paragraph.getEnd() + "'}) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasParagraph + "]->(p)";

				// Continue adding successor relationship.
				if (previousParagraph != null)
				{
					paragraphQuery += "MERGE (p_prev)-[:" + Relationship.NextParagraph + "]->(p)";
				}

				tx.run(paragraphQuery);

				previousParagraph = paragraph;


				Sentence previousSentence = null;
				for (Sentence sentence : JCasUtil.selectCovered(
						document,
						Sentence.class, paragraph
				))
				{
					this.storeSentence(sentence, document, paragraph, previousSentence);
					previousSentence = sentence;
				}
			}
			return 1;
		});
	}

	@Override
	public void storeParagraph(Paragraph paragraph, JCas document, Paragraph previousParagraph)
	{

	}

	@Override
	public void storeParagraph(Paragraph paragraph, JCas document)
	{
		
	}

	/**
	 *
	 * @param sentence The Sentence.
	 * @param document The Document in which the entence occurs.
	 * @param paragraph The Paragraph, in which the Sentence occurs.
	 * @param previousSentence The predecessing Sentence.
	 */
	@Override
	public void storeSentence(Sentence sentence, JCas document, Paragraph paragraph, Sentence previousSentence)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		try (Session session = this.driver.session())
		{
			session.writeTransaction(tx -> {
				// Create sentence (if not exists) and add relationship from
				// document and to paragraph.
				String sentenceQuery = "MATCH (d:" + Label.Document + " {id:'" + documentId + "'}) "
						+ "MATCH (p:" + Label.Paragraph + " {id:'" + documentId + "', begin:'" + paragraph.getBegin() + "', end:'" + paragraph.getEnd() + "'}) ";

				// Add successor relationship from previous sentence (if
				// exists) to current sentence.
				// Match has to be done before the first Merge, so this is split
				// into two parts.
				if (previousSentence != null)
				{
					sentenceQuery += "MATCH (s_prev:" + Label.Sentence + " {id:'" + documentId + "', begin:'" + previousSentence.getBegin() + "', end:'" + previousSentence.getEnd() + "'}) ";
				}

				sentenceQuery += "MERGE (s:" + Label.Sentence + " {id:'" + documentId + "', begin:'" + sentence.getBegin() + "', end:'" + sentence.getEnd() + "'}) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasSentence + "]->(s) "
						+ "MERGE (s)-[:" + Relationship.SentenceInParagraph + "]->(p)";

				// Continue adding successor relationship.
				if (previousSentence != null)
				{
					sentenceQuery += "MERGE (s_prev)-[:" + Relationship.NextSentence + "]->(s)";
				}

				tx.run(sentenceQuery);
				tx.success();

				Token previousToken = null;
				for (Token token : JCasUtil.selectCovered(document, Token.class, sentence))
				{
					this.storeToken(token, document, paragraph, sentence, previousToken);
					previousToken = token;
				}
				return 1;
			});
		}
	}

	/**
	 *
	 * @param sentence The Sentence.
	 * @param document The Document in which the entence occurs.
	 * @param paragraph The Paragraph, in which the Sentence occurs.
	 */
	@Override
	public void storeSentence(Sentence sentence, JCas document, Paragraph paragraph)
	{
		this.storeSentence(sentence, document, paragraph, null);
	}

	/**
	 *  @param token The Token.
	 * @param document The id of the document in which the Token occurs.
	 * @param paragraph The paragraph, in which the Token occurs.
	 * @param sentence The sentence, in which the Token occurs.
	 * @param previousToken The predecessing Token.
	 */
	@Override
	public void storeToken(Token token, JCas document, Paragraph paragraph, Sentence sentence, Token previousToken)
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		try (Session session = this.driver.session())
		{
			session.writeTransaction(tx -> {
				// Create token (if not exists) and add relationship
				// from Document and to Paragraph and to Sentence.
				// Also create Lemma and Pos and add relationships from
				// Token to them as well as from Document to Lemma.
				String tokenQuery = "MATCH (d:" + Label.Document + " {id:'" + documentId + "'}) "
						+ "MATCH (p:" + Label.Paragraph + " {id:'" + documentId + "', begin:'" + paragraph.getBegin() + "', end:'" + paragraph.getEnd() + "'}) "
						+ "MATCH (s:" + Label.Sentence + " {id:'" + documentId + "', begin:'" + sentence.getBegin() + "', end:'" + sentence.getEnd() + "'}) ";

				if (previousToken != null)
				{
					tokenQuery += "MATCH (t_prev:" + Label.Token + " {id:'" + documentId + "', begin:'" + previousToken.getBegin() + "', end:'" + previousToken.getEnd() + "', value='" + previousToken.getCoveredText() + "'}) ";
				}

				tokenQuery += "MERGE (t:" + Label.Token + " {id:'" + documentId + "', begin:'" + token.getBegin() + "', end:'" + token.getEnd() + "', value:'" + token.getCoveredText() + "'}) "
						+ "MERGE (pos:" + Label.Pos + " {value:'" + token.getPos().getPosValue() + "'}) "
						+ "MERGE (l:" + Label.Lemma + " {value:'" + token.getLemma().getValue() + "'}) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasToken + "]->(t) "
						+ "MERGE (t)-[:" + Relationship.TokenInParagraph + "]->(p) "
						+ "MERGE (t)-[:" + Relationship.TokenInSentence + "]->(s) "
						+ "MERGE (t)-[:" + Relationship.TokenHasLemma + "]->(l) "
						+ "MERGE (t)-[:" + Relationship.TokenAtPos + "]->(pos) "
						+ "MERGE (d)-[:" + Relationship.DocumentHasLemma + "]->(l)";

				if (previousToken != null)
				{
					tokenQuery += "MERGE (t_prev)-[:" + Relationship.NextToken + "]->(t)";
				}

				tx.run(tokenQuery);
				return 1;
			});
		}
	}

	/**
	 *  @param token The Token.
	 * @param document The id of the document in which the Token occurs.
	 * @param paragraph The paragraph, in which the Token occurs.
	 * @param sentence The sentence, in which the Token occurs.
	 */
	@Override
	public void storeToken(Token token, JCas document, Paragraph paragraph, Sentence sentence)
	{
		storeToken(token, document, paragraph, sentence, null);
	}

	@Override
	public void storeJCasDocuments(Iterable<JCas> documents)
	{

	}

	@Override
	public void getDocumentsAsJCas()
	{

	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		return 0;
	}

	@Override
	public int countElementsOfType(Const.TYPE type)
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfType(String documentId, Const.TYPE type)
	{
		return 0;
	}

	@Override
	public int countElementsOfTypeWithValue(Const.TYPE type, String value) throws IllegalArgumentException
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(String documentId, Const.TYPE type, String value) throws IllegalArgumentException
	{
		return 0;
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		return null;
	}

	@Override
	public Map<String, Double> calculateTTRForDocument(String documentId)
	{
		return null;
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(Collection<String> documentIds)
	{
		return null;
	}

	@Override
	public double calculateTermFrequencyWithDoubleNormForLemmaInDocument(String lemma, String documentId)
	{
		return 0;
	}

	@Override
	public double calculateTermFrequencyWithLogNermForLemmaInDocument(String lemma, String documentId)
	{
		return 0;
	}

	@Override
	public Map<String, Double> calculateTermFrequenciesForLemmataInDocument(String documentId)
	{
		return null;
	}

	@Override
	public double calculateInverseDocumentFrequency(String lemma)
	{
		return 0;
	}

	@Override
	public Map<String, Double> calculateInverseDocumentFrequenciesForLemmataInDocument(String documentId)
	{
		return null;
	}

	@Override
	public double calculateTFIDFForLemmaInDocument(String lemma, String documentId)
	{
		return 0;
	}

	@Override
	public Map<String, Double> calculateTFIDFForLemmataInDocument(String documentId)
	{
		return null;
	}

	@Override
	public Map<String, Map<String, Double>> calculateTFIDFForLemmataInAllDocuments()
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId) throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments() throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId) throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments() throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public JSONObject call() throws Exception
	{
		return null;
	}
}
