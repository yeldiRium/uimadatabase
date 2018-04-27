package org.hucompute.services.uima.eval.database.abstraction.implementation;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeNotCountableException;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class BlazegraphQueryHandler extends AbstractQueryHandler
{
	protected String rootEndpoint;

	protected enum Prefix
	{
		Document("http://hucompute.org/TextImager/Document#"),
		Paragraph("http://hucompute.org/TextImager/Paragraph#"),
		Sentence("http://hucompute.org/TextImager/Sentence#"),
		Token("http://hucompute.org/TextImager/Token#"),
		Lemma("http://hucompute.org/TextImager/Lemma#"),
		Pos("http://hucompute.org/TextImager/Pos#");

		protected String url;

		Prefix(String url)
		{
			this.url = url;
		}

		public String url()
		{
			return this.url;
		}
	}

	public BlazegraphQueryHandler(String rootEndpoint)
	{
		this.rootEndpoint = rootEndpoint;
	}

	/**
	 * @param query The query to send to the server.
	 * @return a Connection to the SparQL API.
	 */
	protected Connection queryConnection(String query)
	{
		try
		{
			String encodedQuery = URLEncoder.encode(query, "UTF-8");
			String url = this.rootEndpoint + "/bigdata/sparql"
					+ "?query=" + encodedQuery
					+ "&format=json";

			return Jsoup.connect(url)
					.timeout(0)
					.maxBodySize(0);

		// UTF-8 is supported. Exception will not occur.
		} catch (UnsupportedEncodingException ignored)
		{
			return null;
		}
	}

	/**
	 * Creates a connection with a body.
	 * This only has an effect, if post() or put() are used on the connection.
	 *
	 * @param body The request body.
	 * @return a Connection to the SparQL API.
	 */
	protected Connection postConnection(String body)
	{
		String url = this.rootEndpoint + "/bigdata/sparql?format=json";

		return Jsoup.connect(url)
				.requestBody(body)
				.header("Content-Type", "application/sparql-update")
				.timeout(0)
				.maxBodySize(0);
	}

	protected JSONObject getResult(Document document)
	{
		return new JSONObject(document.body()).getJSONObject("results");
	}

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.Blazegraph;
	}

	@Override
	public void setUpDatabase() throws IOException
	{

	}

	@Override
	public void openDatabase() throws IOException
	{

	}

	@Override
	public void clearDatabase() throws IOException
	{
		this.postConnection("CLEAR ALL").post();
	}

	@Override
	public String storeJCasDocument(JCas document) throws QHException
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		final String turtle = "PREFIX document: <" + Prefix.Document.url() + ">\n"
				+ "INSERT DATA {\n"
				+ "document:" + documentId + " document:documentId \"" + documentId + "\" ;\n"
				+ "                            document:text \"\"\"" + document.getDocumentText() + "\"\"\" ;\n"
				+ "                            document:language \"" + document.getDocumentLanguage() + "\" .\n"
				+ "}";

		try
		{
			this.postConnection(turtle).post();
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}

		return documentId;
	}

	@Override
	public String storeParagraph(Paragraph paragraph, String documentId, String previousParagraphId)
	{
		return null;
	}

	@Override
	public String storeSentence(Sentence sentence, String documentId, String paragraphId, String previousSentenceId)
	{
		return null;
	}

	@Override
	public String storeToken(Token token, String documentId, String paragraphId, String sentenceId, String previousTokenId)
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
	public Set<String> getLemmataForDocument(String documentId) throws DocumentNotFoundException
	{
		return null;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId) throws DocumentNotFoundException, QHException
	{

	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		return 0;
	}

	@Override
	public int countElementsOfType(ElementType type) throws TypeNotCountableException
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfType(String documentId, ElementType type) throws DocumentNotFoundException, TypeNotCountableException
	{
		return 0;
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value) throws TypeNotCountableException, TypeHasNoValueException
	{
		return 0;
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(String documentId, ElementType type, String value) throws DocumentNotFoundException, TypeNotCountableException, TypeHasNoValueException
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
	public Double calculateTTRForDocument(String documentId) throws DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(Collection<String> documentIds)
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
	public Iterable<String> getBiGramsFromDocument(String documentId) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments() throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments() throws UnsupportedOperationException
	{
		return null;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return null;
	}
}
