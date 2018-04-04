package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.datastax.driver.core.Session;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.connection.Connections;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class CassandraQueryHandler extends AbstractQueryHandler
{
	protected Session session;

	public CassandraQueryHandler(Session session)
	{
		this.session = session;
	}

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.Cassandra;
	}

	@Override
	public void setUpDatabase()
	{
		String query = "CREATE KEYSPACE IF NOT EXISTS " +
				System.getenv("CASSANDRA_DB") + " WITH replication = {" +
				"'class':'SimpleStrategy'," +
				"'replication_factor':1" +
				"};";
		session.execute(query);

		session.execute("USE " + System.getenv("CASSANDRA_DB"));

		query = "DROP TABLE IF EXISTS \"" + ElementType.Document + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"" + ElementType.Paragraph + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"" + ElementType.Sentence + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"" + ElementType.Token + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"" + ElementType.Lemma + "\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"tokenLemmaMap\"";
		session.execute(query);
		query = "DROP TABLE IF EXISTS \"documentLemmaMap\"";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Document + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"text\" VARCHAR, " +
				"  \"language\" VARCHAR, " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Paragraph + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"documentId\" VARCHAR, " +
				"  \"previousParagraphId\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Sentence + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"paragraphId\" VARCHAR, " +
				"  \"documentId\" VARCHAR, " +
				"  \"previousSentenceId\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Token + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"sentenceId\" VARCHAR, " +
				"  \"paragraphId\" VARCHAR, " +
				"  \"documentId\" VARCHAR, " +
				"  \"previousTokenId\" VARCHAR, " +
				"  \"value\" VARCHAR, " +
				"  \"begin\" INT, " +
				"  \"end\" INT, " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"" + ElementType.Lemma + "\" ( " +
				"  \"uid\" VARCHAR primary key, " +
				"  \"value\" VARCHAR, " +
				")";
		session.execute(query);


		query = "CREATE TABLE \"tokenLemmaMap\" ( " +
				"  \"uid\" uuid primary key, " +
				"  \"tokenId\" VARCHAR, " +
				"  \"lemmaId\" VARCHAR " +
				")";
		session.execute(query);

		query = "CREATE TABLE \"documentLemmaMap\" ( " +
				"  \"uid\" uuid primary key, " +
				"  \"documentId\" VARCHAR, " +
				"  \"lemmaId\" VARCHAR " +
				")";
		session.execute(query);
	}

	@Override
	public void openDatabase() throws IOException
	{
		session.execute("USE " + System.getenv("CASSANDRA_DB"));
	}

	@Override
	public void clearDatabase()
	{
		this.setUpDatabase();
	}

	@Override
	public String storeJCasDocument(JCas document)
	{
		return null;
	}

	@Override
	public String storeParagraph(
			Paragraph paragraph,
			String documentId,
			String previousParagraphId
	)
	{
		return null;
	}

	@Override
	public String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId,
			String previousSentenceId
	)
	{
		return null;
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
		return null;
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
}
