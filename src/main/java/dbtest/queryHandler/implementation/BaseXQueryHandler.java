package dbtest.queryHandler.implementation;

import dbtest.queryHandler.AbstractQueryHandler;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.QHException;
import dbtest.queryHandler.exceptions.TypeHasNoValueException;
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

	/**
	 * Creates an empty database.
	 */
	@Override
	public void setUpDatabase() throws IOException
	{
		this.clientSession.execute(new CreateDB(this.dbName));
		this.clientSession.execute(new Open(this.dbName));
	}

	@Override
	public void clearDatabase() throws IOException
	{
		this.clientSession.execute(new Delete("*"));
	}

	@Override
	public void storeJCasDocument(JCas document) throws QHException
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
	}

	@Override
	public void storeJCasDocuments(Iterable<JCas> documents) throws QHException
	{
		for (JCas document : documents)
		{
			this.storeJCasDocument(document);
		}
	}

	@Override
	public void storeParagraph(
			Paragraph paragraph, JCas document, Paragraph previousParagraph
	)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void storeParagraph(Paragraph paragraph, JCas document)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph,
			Sentence previousSentence
	)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph
	)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void storeToken(
			Token token,
			JCas document,
			Paragraph paragraph,
			Sentence sentence,
			Token previousToken
	)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void storeToken(
			Token token, JCas document, Paragraph paragraph, Sentence sentence
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
				"for $lemma in fn:doc($docId)//type4:Lemma/@value return string($lemma)";

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
