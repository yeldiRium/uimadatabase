package dbtest.queryHandler.implementation;

import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.QHException;
import dbtest.queryHandler.exceptions.TypeNotCountableException;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Wraps any other QueryHandlerInterface.
 * Tunnels all calls and wraps them in a benchmark operation.
 * Adds methods to retrieve the benchmark results.
 */
public class BenchmarkQueryHandler implements QueryHandlerInterface
{
	protected QueryHandlerInterface subjectQueryHandler;

	public BenchmarkQueryHandler(QueryHandlerInterface subjectQueryHandler)
	{
		this.subjectQueryHandler = subjectQueryHandler;
	}

	@Override
	public void setUpDatabase()
	{
		this.subjectQueryHandler.setUpDatabase();
	}

	@Override
	public void clearDatabase()
	{
		this.subjectQueryHandler.clearDatabase();
	}

	@Override
	public void storeJCasDocument(JCas document)
	{
		this.subjectQueryHandler.storeJCasDocument(document);
	}

	@Override
	public void storeJCasDocuments(Iterable<JCas> documents)
	{
		this.subjectQueryHandler.storeJCasDocuments(documents);
	}

	@Override
	public void storeParagraph(
			Paragraph paragraph, JCas document, Paragraph previousParagraph
	)
	{
		this.subjectQueryHandler.storeParagraph(
				paragraph, document, previousParagraph
		);
	}

	@Override
	public void storeParagraph(Paragraph paragraph, JCas document)
	{
		this.subjectQueryHandler.storeParagraph(paragraph, document);
	}

	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph,
			Sentence previousSentence
	)
	{
		this.subjectQueryHandler.storeSentence(
				sentence, document, paragraph, previousSentence
		);
	}

	@Override
	public void storeSentence(
			Sentence sentence, JCas document, Paragraph paragraph
	)
	{
		this.subjectQueryHandler.storeSentence(
				sentence, document, paragraph
		);
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
		this.subjectQueryHandler.storeToken(
				token, document, paragraph, sentence, previousToken
		);
	}

	@Override
	public void storeToken(
			Token token, JCas document, Paragraph paragraph, Sentence sentence
	)
	{
		this.subjectQueryHandler.storeToken(
				token, document, paragraph, sentence
		);
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		return this.subjectQueryHandler.getDocumentIds();
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
	{
		return this.subjectQueryHandler.getLemmataForDocument(documentId);
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{
		this.subjectQueryHandler.populateCasWithDocument(
				aCAS, documentId
		);
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		return this.subjectQueryHandler.countDocumentsContainingLemma(lemma);
	}

	@Override
	public int countElementsOfType(ElementType type)
	{
		return this.subjectQueryHandler.countElementsOfType(type);
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId, ElementType type
	) throws DocumentNotFoundException, TypeNotCountableException
	{
		return this.subjectQueryHandler.countElementsInDocumentOfType(
				documentId, type
		);
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws IllegalArgumentException
	{
		return this.subjectQueryHandler.countElementsOfTypeWithValue(
				type, value
		);
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException, TypeNotCountableException
	{
		return this.subjectQueryHandler.countElementsInDocumentOfTypeWithValue(
				documentId, type, value
		);
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		return this.subjectQueryHandler
				.countOccurencesForEachLemmaInAllDocuments();
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		return this.subjectQueryHandler.calculateTTRForAllDocuments();
	}

	@Override
	public Double calculateTTRForDocument(String documentId)
			throws DocumentNotFoundException
	{
		return this.subjectQueryHandler.calculateTTRForDocument(documentId);
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(
			Collection<String> documentIds
	)
	{
		return this.subjectQueryHandler.calculateTTRForCollectionOfDocuments(
				documentIds
		);
	}

	@Override
	public double calculateTermFrequencyWithDoubleNormForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException
	{
		return this.subjectQueryHandler
				.calculateTermFrequencyWithDoubleNormForLemmaInDocument(
						lemma, documentId
				);
	}

	@Override
	public double calculateTermFrequencyWithLogNormForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException
	{
		return this.subjectQueryHandler
				.calculateTermFrequencyWithLogNormForLemmaInDocument(
						lemma, documentId
				);
	}

	@Override
	public Map<String, Double> calculateTermFrequenciesForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		return this.subjectQueryHandler
				.calculateTermFrequenciesForLemmataInDocument(documentId);
	}

	@Override
	public double calculateInverseDocumentFrequency(String lemma)
	{
		return this.subjectQueryHandler
				.calculateInverseDocumentFrequency(lemma);
	}

	@Override
	public Map<String, Double>
	calculateInverseDocumentFrequenciesForLemmataInDocument(String documentId)
			throws DocumentNotFoundException
	{
		return this.subjectQueryHandler
				.calculateInverseDocumentFrequenciesForLemmataInDocument(
						documentId
				);
	}

	@Override
	public double calculateTFIDFForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException
	{
		return this.subjectQueryHandler.calculateTFIDFForLemmaInDocument(
				lemma, documentId
		);
	}

	@Override
	public Map<String, Double> calculateTFIDFForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		return this.subjectQueryHandler.calculateTFIDFForLemmataInDocument(
				documentId
		);
	}

	@Override
	public Map<String, Map<String, Double>>
	calculateTFIDFForLemmataInAllDocuments()
	{
		return this.subjectQueryHandler
				.calculateTFIDFForLemmataInAllDocuments();
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(
			String documentId
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return this.subjectQueryHandler.getBiGramsFromDocument(
				documentId
		);
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		return this.subjectQueryHandler.getBiGramsFromAllDocuments();
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return this.subjectQueryHandler.getBiGramsFromDocumentsInCollection(
				documentIds
		);
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		return this.subjectQueryHandler.getTriGramsFromDocument(
				documentId
		);
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		return this.subjectQueryHandler.getTriGramsFromAllDocuments();
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		return this.subjectQueryHandler.getTriGramsFromDocumentsInCollection(
				documentIds
		);
	}
}
