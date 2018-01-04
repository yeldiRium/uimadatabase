package org.hucompute.services.uima.database.neo4j;

import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.database.AbstractQueryHandler;
import org.hucompute.services.uima.database.Const;
import org.json.JSONObject;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.util.*;

public class Neo4jQueryHandlerNew extends AbstractQueryHandler
{
	protected Driver driver;

	public enum Label {
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

	@Override
	public void storeJCasDocument(JCas document)
	{

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
