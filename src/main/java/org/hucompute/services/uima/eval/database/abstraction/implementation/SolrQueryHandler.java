package org.hucompute.services.uima.eval.database.abstraction.implementation;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeNotCountableException;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

public class SolrQueryHandler extends AbstractQueryHandler
{
	protected String rootEndpoint;

	public SolrQueryHandler(String endpoint)
	{
		this.rootEndpoint = endpoint + "/solr/" + System.getenv("SOLR_CORE");
	}

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.Solr;
	}

	/**
	 * Nothing to do here.
	 *
	 * @throws IOException
	 */
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
		Request.Post(this.rootEndpoint + "/update")
				.bodyString(
						"<delete><query>*:*</query></delete>",
						ContentType.create("text/xml")
				)
				.execute();
	}

	@Override
	public String storeJCasDocument(JCas document) throws QHException
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();

		String url = this.rootEndpoint + "/update";

		JSONObject command = new JSONObject();
		JSONObject add = new JSONObject();
		command.put("add", add);
		JSONObject doc = new JSONObject();
		add.put("doc", doc);
		doc.put("id", documentId);
		doc.put("text", document.getDocumentText());

		try
		{
			Request.Post(url)
					.addHeader("Accept", "application/json")
					.bodyString(
							command.toString(),
							ContentType.create("application/json")
					)
					.execute();
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
		throw new UnsupportedOperationException();
	}

	@Override
	public String storeSentence(Sentence sentence, String documentId, String paragraphId, String previousSentenceId)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public String storeToken(Token token, String documentId, String paragraphId, String sentenceId, String previousTokenId)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkIfDocumentExists(String documentId) throws DocumentNotFoundException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId) throws DocumentNotFoundException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId) throws DocumentNotFoundException, QHException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int countElementsOfType(ElementType type) throws TypeNotCountableException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int countElementsInDocumentOfType(String documentId, ElementType type) throws DocumentNotFoundException, TypeNotCountableException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value) throws TypeNotCountableException, TypeHasNoValueException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(String documentId, ElementType type, String value) throws DocumentNotFoundException, TypeNotCountableException, TypeHasNoValueException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Double calculateTTRForDocument(String documentId) throws DocumentNotFoundException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(Collection<String> documentIds)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(String documentId) throws DocumentNotFoundException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId) throws UnsupportedOperationException, DocumentNotFoundException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments() throws UnsupportedOperationException
	{
		List<String> biGrams = new ArrayList<>();
		try
		{
			JSONObject response = new JSONObject(
					Request.Get(this.rootEndpoint + "/terms?terms.fl=biGrams&terms.limit=-1&terms.sort=index")
							.addHeader("Accept", "application/json")
							.execute()
							.returnContent()
							.toString()
			);

			JSONArray terms = response
					.getJSONObject("terms")
					.getJSONArray("biGrams");

			// Steps of two, since every second element is the frequency of the
			// biGram before.
			for (int i = 0; i < terms.length(); i += 2)
			{
				biGrams.add(terms.getString(i));
			}

			return biGrams;
		} catch (IOException e)
		{
			e.printStackTrace();
			return biGrams;
		}
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException, DocumentNotFoundException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId) throws UnsupportedOperationException, DocumentNotFoundException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments() throws UnsupportedOperationException
	{
		List<String> triGrams = new ArrayList<>();
		try
		{
			JSONObject response = new JSONObject(
					Request.Get(this.rootEndpoint + "/terms?terms.fl=triGrams&terms.limit=-1&terms.sort=index")
							.addHeader("Accept", "application/json")
							.execute()
							.returnContent()
							.toString()
			);

			JSONArray terms = response
					.getJSONObject("terms")
					.getJSONArray("triGrams");

			// Steps of two, since every second element is the frequency of the
			// biGram before.
			for (int i = 0; i < terms.length(); i += 2)
			{
				triGrams.add(terms.getString(i));
			}

			return triGrams;
		} catch (IOException e)
		{
			e.printStackTrace();
			return triGrams;
		}
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(Collection<String> documentIds) throws UnsupportedOperationException, DocumentNotFoundException
	{
		throw new UnsupportedOperationException();
	}
}
