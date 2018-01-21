package dbtest.queryHandler.implementation;

import dbtest.queryHandler.AbstractQueryHandler;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.QHException;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.io.xmi.XmiWriter;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.basex.api.client.ClientSession;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.Delete;
import org.basex.core.cmd.DropDB;
import org.basex.core.cmd.Retrieve;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;

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

	/**
	 * Creates an empty database.
	 */
	@Override
	public void setUpDatabase() throws IOException
	{
		this.clientSession.execute(new CreateDB(this.dbName));
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
			PipedInputStream input = new PipedInputStream();
			PipedOutputStream output = new PipedOutputStream();
			output.connect(input);

			XmiCasSerializer.serialize(document.getCas(), output);
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
		try
		{
			if (this.clientSession.execute(new Retrieve(documentId)) == null)
				throw new DocumentNotFoundException();
		} catch (IOException e)
		{
			throw new QHException(e);
		}
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
		this.checkIfDocumentExists(documentId);
		try
		{
			PipedInputStream input = new PipedInputStream();
			PipedOutputStream output = new PipedOutputStream();
			output.connect(input);
			this.clientSession.setOutputStream(output);
			this.clientSession.execute(new Retrieve(documentId));

			XmiCasDeserializer.deserialize(input, aCAS);
			this.clientSession.setOutputStream(null);
		} catch (SAXException | IOException e)
		{
			throw new QHException(e);
		}
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
