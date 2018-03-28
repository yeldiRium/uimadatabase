package org.hucompute.services.uima.eval.database.abstraction.implementation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.hucompute.services.uima.eval.database.abstraction.AbstractQueryHandler;
import org.hucompute.services.uima.eval.database.abstraction.ElementType;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.DocumentNotFoundException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.TypeHasNoValueException;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.jcas.JCas;
import org.basex.api.client.ClientQuery;
import org.basex.api.client.ClientSession;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.Delete;
import org.basex.core.cmd.Open;
import org.hucompute.services.uima.eval.database.abstraction.exceptions.QHException;
import org.hucompute.services.uima.eval.database.connection.Connections;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * In BaseX, only full files can be added to the database.
 * Thus the only implemented storage method is storeJCasDocument.
 * All the other storage methods will throw UnsupportedOperationExceptions.
 */
public class BaseXQueryHandler extends AbstractQueryHandler
{
	protected ClientSession clientSession;
	protected final String dbName = System.getenv("BASEX_DBNAME");

	public BaseXQueryHandler(ClientSession clientSession)
	{
		this.clientSession = clientSession;
	}

	protected String getDocumentIdFromUri(String uri)
	{
		return uri.replace(this.dbName + "/", "");
	}

	protected String getUriFromDocumentId(String documentId)
	{
		return this.dbName + "/" + documentId;
	}

	@Override
	public Connections.DBName forConnection()
	{
		return Connections.DBName.BaseX;
	}

	/**
	 * Creates an empty database.
	 */
	@Override
	public void setUpDatabase() throws IOException
	{
		this.clientSession.execute(new CreateDB(this.dbName));
	}

	@Override
	public void openDatabase() throws IOException
	{
		this.clientSession.execute(new Open(this.dbName));
	}

	@Override
	public void clearDatabase() throws IOException
	{
		this.clientSession.execute(new Delete("*"));
	}

	@Override
	public String storeJCasDocument(JCas document) throws QHException
	{
		final String documentId = DocumentMetaData.get(document)
				.getDocumentId();
		try
		{
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			XmiCasSerializer.serialize(document.getCas(), output);
			ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());

			this.clientSession.add(documentId, input);
		} catch (SAXException | IOException e)
		{
			throw new QHException(e);
		}

		return documentId;
	}

	@Override
	public String storeParagraph(
			Paragraph paragraph, String documentId, String paragraphId
	)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public String storeParagraph(Paragraph paragraph, String documentId)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId,
			String previousSentenceId
	)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public String storeSentence(
			Sentence sentence,
			String documentId,
			String paragraphId
	)
	{
		throw new UnsupportedOperationException();
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
		throw new UnsupportedOperationException();
	}

	@Override
	public String storeToken(
			Token token,
			String documentId,
			String paragraphId,
			String sentenceId
	)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkIfDocumentExists(String documentId) throws DocumentNotFoundException
	{
		this.clientSession.setOutputStream(null);
		String queryString = "declare variable $doc as xs:string external; " +
				"fn:doc-available($doc)";
		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$doc", this.getUriFromDocumentId(documentId));
			if (!Boolean.parseBoolean(query.execute()))
			{
				throw new DocumentNotFoundException();
			}
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		String queryString = "for $doc in fn:collection() return fn:document-uri($doc)";
		ArrayList<String> documentIds = new ArrayList<>();
		try (ClientQuery query = this.clientSession.query(queryString))
		{
			while (query.more())
			{
				documentIds.add(this.getDocumentIdFromUri(query.next()));
			}
			return documentIds;
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"declare variable $docId as xs:string external; " +
				"for $lemma in fn:distinct-values(fn:doc($docId)//type4:Lemma/@value) return string($lemma)";

		Set<String> lemmata = new TreeSet<>();
		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$docId", this.getUriFromDocumentId(documentId));

			while (query.more())
			{
				lemmata.add(query.next());
			}

			return lemmata;
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{
		this.checkIfDocumentExists(documentId);
		String queryString = "declare variable $doc as xs:string external; " +
				"fn:doc($doc)";
		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$doc", this.getUriFromDocumentId(documentId));

			String documentXmi = query.execute();
			InputStream input = new ByteArrayInputStream(
					documentXmi.getBytes()
			);

			XmiCasDeserializer.deserialize(input, aCAS);
		} catch (SAXException | IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"declare variable $lemma as xs:string external; " +
				"fn:count( " +
				"    for $doc in fn:collection() " +
				"        where fn:exists($doc//type4:Lemma[@value = $lemma]) " +
				"        return 1 " +
				")";

		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$lemma", lemma);

			return Integer.parseInt(query.execute());
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		String queryString = null;
		switch (type)
		{
			case Document:
				queryString = "fn:count(fn:collection())";
				break;
			case Paragraph:
			case Sentence:
			case Token:
			case Pos:
				// Pos aren't separate elements in XMI but attributes on Token.
				// So by counting Tokens we implicitly count Pos.
				String typeName = (type == ElementType.Pos)
						? ElementType.Token.toString() : type.toString();
				queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
						"fn:count( " +
						"    //type4:" + typeName +
						")";
				break;
			case Lemma:
				// Lemmata should be distinct
				queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
						"fn:count(fn:distinct-values( " +
						"   //type4:Lemma/@value" +
						"))";
		}
		try (ClientQuery query = this.clientSession.query(queryString))
		{
			return Integer.parseInt(query.execute());
		} catch (IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId, ElementType type
	) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String queryString = null;
		switch (type)
		{
			case Document:
				return 1;
			case Paragraph:
			case Sentence:
			case Token:
			case Pos:
				// Pos aren't separate elements in XMI but attributes on Token.
				// So by counting Tokens we implicitly count Pos.
				String typeName = (type == ElementType.Pos)
						? ElementType.Token.toString() : type.toString();
				queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
						"declare variable $docId as xs:string external; " +
						"fn:count( " +
						"    fn:doc($docId)//type4:" + typeName +
						")";
				break;
			case Lemma:
				// Lemmata should be distinct
				queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
						"declare variable $docId as xs:string external; " +
						"fn:count(fn:distinct-values( " +
						"   fn:doc($docId)//type4:Lemma/@value" +
						"))";
		}
		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$docId", this.getUriFromDocumentId(documentId));
			return Integer.parseInt(query.execute());
		} catch (IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws IllegalArgumentException, TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);

		String queryString = null;
		switch (type)
		{
			case Document:
			case Paragraph:
			case Sentence:
				// Will never happen, since these cases throw an exception in
				// the check above.
				return 0;
			case Pos:
				// Pos values aren't really queryable or even relevant.
				return 0;
			case Token:
				queryString = "declare namespace xmi = 'http://www.omg.org/XMI'; " +
						"declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
						"declare variable $value as xs:string external; " +
						"fn:count(" +
						"    for $lemmaId in //type4:Lemma[@value = $value]/@xmi:id " +
						"        return //type4:Token[@lemma = string($lemmaId)] " +
						")";
				break;
			case Lemma:
				// Lemmata should be distinct
				queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
						"declare variable $value as xs:string external; " +
						"fn:count(fn:distinct-values( " +
						"    //type4:Lemma[@value = $value] " +
						"))";
		}
		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$value", value);
			return Integer.parseInt(query.execute());
		} catch (IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException, TypeHasNoValueException
	{
		this.checkTypeHasValueField(type);
		this.checkIfDocumentExists(documentId);

		String queryString = null;
		switch (type)
		{
			case Document:
			case Paragraph:
			case Sentence:
				// Will never happen, since these cases throw an exception in
				// the check above.
				return 0;
			case Pos:
				// Pos values aren't really queryable or even relevant.
				return 0;
			case Token:
				queryString = "declare namespace xmi = 'http://www.omg.org/XMI'; " +
						"declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
						"declare variable $value as xs:string external; " +
						"declare variable $docId as xs:string external; " +
						"let $doc := fn:doc($docId) " +
						"return fn:count(" +
						"    for $lemmaId in $doc//type4:Lemma[@value = $value]/@xmi:id " +
						"        return $doc//type4:Token[@lemma = string($lemmaId)] " +
						")";
				break;
			case Lemma:
				// Lemmata should be distinct
				queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
						"declare variable $value as xs:string external; " +
						"declare variable $docId as xs:string external; " +
						"fn:count(fn:distinct-values( " +
						"    fn:doc($docId)//type4:Lemma[@value = $value] " +
						"))";
		}
		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$docId", this.getUriFromDocumentId(documentId));
			query.bind("$value", value);
			return Integer.parseInt(query.execute());
		} catch (IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		Map<String, Integer> occurenceMap = new TreeMap<>();
		String valueQueryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"fn:distinct-values(//type4:Lemma/@value)";
		try (ClientQuery valueQuery =
				     this.clientSession.query(valueQueryString))
		{
			while (valueQuery.more())
			{
				String value = valueQuery.next();
				String occurenceQueryString = "declare namespace xmi = 'http://www.omg.org/XMI'; " +
						"declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
						"declare variable $value as xs:string external; " +
						"fn:count( " +
						"    for $lemmaId in //type4:Lemma[@value = $value]/@xmi:id " +
						"        return //type4:Token[@lemma = string($lemmaId)]" +
						")";
				try (ClientQuery occurenceQuery =
						     this.clientSession.query(occurenceQueryString))
				{
					occurenceQuery.bind("$value", value);
					occurenceMap.put(
							value, Integer.parseInt(occurenceQuery.execute())
					);
				}
			}
			return occurenceMap;
		} catch (IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		return this.calculateTTRForCollectionOfDocuments(
				Lists.newArrayList(this.getDocumentIds())
		);
	}

	@Override
	public Double calculateTTRForDocument(String documentId)
			throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String queryString = "declare namespace xmi = 'http://www.omg.org/XMI'; " +
				"declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"declare variable $docId as xs:string external; " +
				"let $lemmaCount := fn:count(fn:distinct-values(fn:doc($docId)//type4:Lemma/@value)) " +
				"let $tokenCount := fn:count(fn:doc($docId)//type4:Token) " +
				"return (" +
				"    if ($tokenCount > 0) then " +
				"        ($lemmaCount div $tokenCount) " +
				"    else " +
				"        0 " +
				")";

		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$docId", this.getUriFromDocumentId(documentId));

			return Double.parseDouble(query.execute());
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(
			Collection<String> documentIds
	)
	{
		Map<String, Double> ttrMap = new HashMap<>();
		for (String documentId : documentIds)
		{
			try {
				ttrMap.put(
						documentId, this.calculateTTRForDocument(documentId)
				);
			} catch (DocumentNotFoundException e)
			{
				// missing documents in collections are ignored
			}
		}
		return ttrMap;
	}

	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		Map<String, Integer> frequencyMap = new HashMap<>();
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"declare variable $docId as xs:string external; " +
				"let $doc := fn:doc($docId) " +
				"for $value in fn:distinct-values($doc//type4:Lemma/@value) " +
				"    let $valueString := string($value) " +
				"    return ($value, fn:count($doc//type4:Lemma[@value = $valueString]))";

		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$docId", this.getUriFromDocumentId(documentId));

			// query returns alternatingly lemma values and their frequency
			while (query.more())
			{
				frequencyMap.put(
						query.next(), Integer.parseInt(query.next())
				);
			}
			return frequencyMap;
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"declare variable $docId as xs:string external; " +
				"declare variable $lemma as xs:string external; " +
				"fn:count(fn:doc($docId)//type4:Lemma[@value = $lemma])";

		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$docId", this.getUriFromDocumentId(documentId));
			query.bind("$lemma", lemma);

			return Integer.parseInt(query.execute());
		} catch (IOException e)
		{
			e.printStackTrace();
			throw new QHException(e);
		}
	}

	/**
	 * @param query A query that results in multiple string values.
	 */
	protected Iterable<String> putResultsIntoIterable(
			ClientQuery query
	) throws IOException
	{
		ArrayList<String> list = new ArrayList<>();
		while (query.more())
		{
			list.add(query.next());
		}
		return list;
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"declare variable $docId as xs:string external; " +
				"for $lemma in fn:doc($docId)//type4:Lemma[position() < last()] " +
				"    let $value := string($lemma/@value) " +
				"    let $nextValue := string($lemma/following-sibling::*[1]/@value)" +
				"    return string-join(($value, $nextValue), '-') ";

		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$docId", this.getUriFromDocumentId(documentId));
			return this.putResultsIntoIterable(query);
		} catch (IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"for $lemma in //type4:Lemma[position() < last()] " +
				"    let $value := string($lemma/@value) " +
				"    let $nextValue := string($lemma/following-sibling::*[1]/@value)" +
				"    return string-join(($value, $nextValue), '-') ";

		try (ClientQuery query = this.clientSession.query(queryString))
		{
			return this.putResultsIntoIterable(query);
		} catch (IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"declare variable $docId as xs:string external; " +
				"for $lemma in fn:doc($docId)//type4:Lemma[position() < last()] " +
				"    let $value := string($lemma/@value) " +
				"    let $nextValue := string($lemma/following-sibling::*[1]/@value)" +
				"    return string-join(($value, $nextValue), '-') ";

		Iterable<String> biGrams = null;
		for (String documentId : documentIds)
		{
			try (ClientQuery query = this.clientSession.query(queryString))
			{
				query.bind("$docId", this.getUriFromDocumentId(documentId));
				if (biGrams == null)
				{
					biGrams = this.putResultsIntoIterable(query);
				} else
				{
					biGrams = Iterables.concat(
							biGrams,
							this.putResultsIntoIterable(query)
					);
				}
			} catch (IOException e)
			{
				throw new QHException(e);
			}
		}
		return biGrams;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		this.checkIfDocumentExists(documentId);
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"declare variable $docId as xs:string external; " +
				"for $lemma in fn:doc($docId)//type4:Lemma[position() < (last() - 1)] " +
				"    let $value := string($lemma/@value) " +
				"    let $nextValue := string($lemma/following-sibling::*[1]/@value)" +
				"    let $thirdValue := string($lemma/following-sibling::*[2]/@value)" +
				"    return string-join(($value, $nextValue, $thirdValue), '-') ";

		try (ClientQuery query = this.clientSession.query(queryString))
		{
			query.bind("$docId", this.getUriFromDocumentId(documentId));
			return this.putResultsIntoIterable(query);
		} catch (IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"for $lemma in //type4:Lemma[position() < last()] " +
				"    let $value := string($lemma/@value) " +
				"    let $nextValue := string($lemma/following-sibling::*[1]/@value)" +
				"    let $thirdValue := string($lemma/following-sibling::*[2]/@value)" +
				"    return string-join(($value, $nextValue, $thirdValue), '-') ";

		try (ClientQuery query = this.clientSession.query(queryString))
		{
			return this.putResultsIntoIterable(query);
		} catch (IOException e)
		{
			throw new QHException(e);
		}
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		String queryString = "declare namespace type4 = 'http:///de/tudarmstadt/ukp/dkpro/core/api/segmentation/type.ecore'; " +
				"declare variable $docId as xs:string external; " +
				"for $lemma in fn:doc($docId)//type4:Lemma[position() < last()] " +
				"    let $value := string($lemma/@value) " +
				"    let $nextValue := string($lemma/following-sibling::*[1]/@value)" +
				"    let $thirdValue := string($lemma/following-sibling::*[2]/@value)" +
				"    return string-join(($value, $nextValue, $thirdValue), '-') ";

		Iterable<String> triGrams = null;
		for (String documentId : documentIds)
		{
			try (ClientQuery query = this.clientSession.query(queryString))
			{
				query.bind("$docId", this.getUriFromDocumentId(documentId));
				if (triGrams == null)
				{
					triGrams = this.putResultsIntoIterable(query);
				} else
				{
					triGrams = Iterables.concat(
							triGrams,
							this.putResultsIntoIterable(query)
					);
				}
			} catch (IOException e)
			{
				throw new QHException(e);
			}
		}
		return triGrams;
	}
}
