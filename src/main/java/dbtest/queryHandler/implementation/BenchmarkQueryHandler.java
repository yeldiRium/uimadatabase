package dbtest.queryHandler.implementation;

import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.QHException;
import dbtest.queryHandler.exceptions.TypeHasNoValueException;
import dbtest.queryHandler.exceptions.TypeNotCountableException;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.util.*;

/**
 * Wraps any other QueryHandlerInterface.
 * Tunnels all calls and wraps them in a benchmark operation.
 * Adds methods to retrieve the benchmark results.
 */
public class BenchmarkQueryHandler implements QueryHandlerInterface
{
	public class MethodBenchmark
	{
		protected int callCount = 0;
		protected List<Long> callTimes;

		public MethodBenchmark()
		{
			this.callTimes = new ArrayList<>();
		}

		public int getCallCount()
		{
			return callCount;
		}

		public void increaseCallCount()
		{
			this.callCount++;
		}

		public List<Long> getCallTimes()
		{
			return callTimes;
		}

		public void addCallTime(long callTime)
		{
			this.callTimes.add(callTime);
		}

		public void reset()
		{
			this.callCount = 0;
			this.callTimes = new ArrayList<>();
		}
	}

	protected QueryHandlerInterface subjectQueryHandler;
	protected Map<String, MethodBenchmark> methodBenchmarks;

	public BenchmarkQueryHandler(QueryHandlerInterface subjectQueryHandler)
	{
		this.resetMethodBenchmarks();
		this.subjectQueryHandler = subjectQueryHandler;
	}

	public void resetMethodBenchmarks()
	{
		this.methodBenchmarks = new HashMap<>();
		Arrays.stream(this.getClass().getDeclaredMethods())
				.forEach(method -> {
					this.methodBenchmarks.put(
							method.getName(),
							new MethodBenchmark()
					);
				});
	}

	public Map<String, MethodBenchmark> getMethodBenchmarks()
	{
		return this.methodBenchmarks;
	}

	@Override
	public void setUpDatabase() throws IOException
	{
		long start = System.currentTimeMillis();
		this.subjectQueryHandler.setUpDatabase();
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("setUpDatabase");
		mb.increaseCallCount();
		mb.addCallTime(end - start);
	}

	@Override
	public void clearDatabase() throws IOException
	{
		long start = System.currentTimeMillis();
		this.subjectQueryHandler.clearDatabase();
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("clearDatabase");
		mb.increaseCallCount();
		mb.addCallTime(end - start);
	}

	@Override
	public void storeJCasDocument(JCas document) throws QHException
	{
		long start = System.currentTimeMillis();
		this.subjectQueryHandler.storeJCasDocument(document);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("storeJCasDocument");
		mb.increaseCallCount();
		mb.addCallTime(end - start);
	}

	@Override
	public void storeJCasDocuments(Iterable<JCas> documents)
	{
		long start = System.currentTimeMillis();
		this.subjectQueryHandler.storeJCasDocuments(documents);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("storeJCasDocuments");
		mb.increaseCallCount();
		mb.addCallTime(end - start);
	}

	@Override
	public void storeParagraph(
			Paragraph paragraph, JCas document, Paragraph previousParagraph
	)
	{
		long start = System.currentTimeMillis();
		this.subjectQueryHandler.storeParagraph(
				paragraph, document, previousParagraph
		);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("storeParagraph");
		mb.increaseCallCount();
		mb.addCallTime(end - start);
	}

	/**
	 * Is not benchmarked, since it is only a virtual default method.
	 *
	 * @param paragraph The Paragraph.
	 * @param document  The document in which the paragraph occurs.
	 */
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
		long start = System.currentTimeMillis();
		this.subjectQueryHandler.storeSentence(
				sentence, document, paragraph, previousSentence
		);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("storeSentence");
		mb.increaseCallCount();
		mb.addCallTime(end - start);
	}

	/**
	 * Is not benchmarked, since it is only a virtual default method.
	 *
	 * @param sentence  The Sentence.
	 * @param document  The Document in which the entence occurs.
	 * @param paragraph The Paragraph, in which the Sentence occurs.
	 */
	@Override
	public void storeSentence(
			Sentence sentence, JCas document, Paragraph paragraph
	)
	{
		this.subjectQueryHandler.storeSentence(sentence, document, paragraph);
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
		long start = System.currentTimeMillis();
		this.subjectQueryHandler.storeToken(
				token, document, paragraph, sentence, previousToken
		);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("storeToken");
		mb.increaseCallCount();
		mb.addCallTime(end - start);
	}

	/**
	 * Is not benchmarked, since it is only a virtual default method.
	 *
	 * @param token     The Token.
	 * @param document  The Document in which the Token occurs.
	 * @param paragraph The Paragraph, in which the Token occurs.
	 * @param sentence  The Sentence, in which the Token occurs.
	 */
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
	public void checkIfDocumentExists(String documentId)
			throws DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		this.subjectQueryHandler.checkIfDocumentExists(documentId);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("checkIfDocumentExists");
		mb.increaseCallCount();
		mb.addCallTime(end - start);
	}

	@Override
	public Iterable<String> getDocumentIds()
	{
		long start = System.currentTimeMillis();
		Iterable<String> result = this.subjectQueryHandler.getDocumentIds();
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("getDocumentIds");
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Set<String> getLemmataForDocument(String documentId)
			throws DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		Set<String> result = this.subjectQueryHandler.getLemmataForDocument(
				documentId
		);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("getLemmataForDocument");
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public void populateCasWithDocument(CAS aCAS, String documentId)
			throws DocumentNotFoundException, QHException
	{
		long start = System.currentTimeMillis();
		this.subjectQueryHandler.populateCasWithDocument(
				aCAS, documentId
		);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"populateCasWithDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);
	}

	@Override
	public int countDocumentsContainingLemma(String lemma)
	{
		long start = System.currentTimeMillis();
		int result = this.subjectQueryHandler.countDocumentsContainingLemma(
				lemma
		);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"countDocumentsContainingLemma"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public int countElementsOfType(ElementType type)
			throws TypeNotCountableException
	{
		long start = System.currentTimeMillis();
		int result = this.subjectQueryHandler.countElementsOfType(type);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get("countElementsOfType");
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public int countElementsInDocumentOfType(
			String documentId, ElementType type
	) throws DocumentNotFoundException, TypeNotCountableException
	{
		long start = System.currentTimeMillis();
		int result = this.subjectQueryHandler.countElementsInDocumentOfType(
				documentId, type
		);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"countElementsInDocumentOfType"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public int countElementsOfTypeWithValue(ElementType type, String value)
			throws IllegalArgumentException, TypeHasNoValueException,
			TypeNotCountableException
	{
		long start = System.currentTimeMillis();
		int result = this.subjectQueryHandler.countElementsOfTypeWithValue(
				type, value
		);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"countElementsOfTypeWithValue"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public int countElementsInDocumentOfTypeWithValue(
			String documentId, ElementType type, String value
	) throws DocumentNotFoundException, TypeNotCountableException,
			TypeHasNoValueException
	{
		long start = System.currentTimeMillis();
		int result = this.subjectQueryHandler
				.countElementsInDocumentOfTypeWithValue(
						documentId, type, value
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"countElementsInDocumentOfTypeWithValue"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Map<String, Integer> countOccurencesForEachLemmaInAllDocuments()
	{
		long start = System.currentTimeMillis();
		Map<String, Integer> result = this.subjectQueryHandler
				.countOccurencesForEachLemmaInAllDocuments();
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"countOccurencesForEachLemmaInAllDocuments"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Map<String, Double> calculateTTRForAllDocuments()
	{
		long start = System.currentTimeMillis();
		Map<String, Double> result = this.subjectQueryHandler
				.calculateTTRForAllDocuments();
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateTTRForAllDocuments"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Double calculateTTRForDocument(String documentId)
			throws DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		Double result = this.subjectQueryHandler
				.calculateTTRForDocument(documentId);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateTTRForDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Map<String, Double> calculateTTRForCollectionOfDocuments(
			Collection<String> documentIds
	)
	{
		long start = System.currentTimeMillis();
		Map<String, Double> result = this.subjectQueryHandler
				.calculateTTRForCollectionOfDocuments(
						documentIds
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateTTRForCollectionOfDocuments"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Map<String, Integer> calculateRawTermFrequenciesInDocument(String documentId) throws DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		Map<String, Integer> result = this.subjectQueryHandler
				.calculateRawTermFrequenciesInDocument(
						documentId
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateRawTermFrequenciesInDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Integer calculateRawTermFrequencyForLemmaInDocument(String lemma, String documentId) throws DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		Integer result = this.subjectQueryHandler
				.calculateRawTermFrequencyForLemmaInDocument(
						lemma, documentId
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateRawTermFrequencyForLemmaInDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public double calculateTermFrequencyWithDoubleNormForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		double result = this.subjectQueryHandler
				.calculateTermFrequencyWithDoubleNormForLemmaInDocument(
						lemma, documentId
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateTermFrequencyWithDoubleNormForLemmaInDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public double calculateTermFrequencyWithLogNormForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		double result = this.subjectQueryHandler
				.calculateTermFrequencyWithLogNormForLemmaInDocument(
						lemma, documentId
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateTermFrequencyWithLogNormForLemmaInDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Map<String, Double> calculateTermFrequenciesForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		Map<String, Double> result = this.subjectQueryHandler
				.calculateTermFrequenciesForLemmataInDocument(documentId);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateTermFrequenciesForLemmataInDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public double calculateInverseDocumentFrequency(String lemma)
			throws OperationNotSupportedException
	{
		long start = System.currentTimeMillis();
		double result = this.subjectQueryHandler
				.calculateInverseDocumentFrequency(lemma);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateInverseDocumentFrequency"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Map<String, Double>
	calculateInverseDocumentFrequenciesForLemmataInDocument(String documentId)
			throws DocumentNotFoundException, OperationNotSupportedException
	{
		long start = System.currentTimeMillis();
		Map<String, Double> result = this.subjectQueryHandler
				.calculateInverseDocumentFrequenciesForLemmataInDocument(
						documentId
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateInverseDocumentFrequenciesForLemmataInDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public double calculateTFIDFForLemmaInDocument(
			String lemma, String documentId
	) throws DocumentNotFoundException, OperationNotSupportedException
	{
		long start = System.currentTimeMillis();
		double result = this.subjectQueryHandler
				.calculateTFIDFForLemmaInDocument(
						lemma, documentId
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateTFIDFForLemmaInDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Map<String, Double> calculateTFIDFForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException, OperationNotSupportedException
	{
		long start = System.currentTimeMillis();
		Map<String, Double> result = this.subjectQueryHandler
				.calculateTFIDFForLemmataInDocument(
						documentId
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateTFIDFForLemmataInDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Map<String, Map<String, Double>>
	calculateTFIDFForLemmataInAllDocuments()
			throws OperationNotSupportedException
	{
		long start = System.currentTimeMillis();
		Map<String, Map<String, Double>> result = this.subjectQueryHandler
				.calculateTFIDFForLemmataInAllDocuments();
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"calculateTFIDFForLemmataInAllDocuments"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Iterable<String> getBiGramsFromDocument(
			String documentId
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		Iterable<String> result = this.subjectQueryHandler
				.getBiGramsFromDocument(
						documentId
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"getBiGramsFromDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Iterable<String> getBiGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		long start = System.currentTimeMillis();
		Iterable<String> result = this.subjectQueryHandler
				.getBiGramsFromAllDocuments();
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"getBiGramsFromAllDocuments"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Iterable<String> getBiGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		Iterable<String> result = this.subjectQueryHandler
				.getBiGramsFromDocumentsInCollection(
						documentIds
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"getBiGramsFromDocumentsInCollection"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Iterable<String> getTriGramsFromDocument(String documentId)
			throws UnsupportedOperationException, DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		Iterable<String> result = this.subjectQueryHandler
				.getTriGramsFromDocument(
						documentId
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"getTriGramsFromDocument"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Iterable<String> getTriGramsFromAllDocuments()
			throws UnsupportedOperationException
	{
		long start = System.currentTimeMillis();
		Iterable<String> result = this.subjectQueryHandler
				.getTriGramsFromAllDocuments();
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"getTriGramsFromAllDocuments"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}

	@Override
	public Iterable<String> getTriGramsFromDocumentsInCollection(
			Collection<String> documentIds
	) throws UnsupportedOperationException, DocumentNotFoundException
	{
		long start = System.currentTimeMillis();
		Iterable<String> result = this.subjectQueryHandler
				.getTriGramsFromDocumentsInCollection(
						documentIds
				);
		long end = System.currentTimeMillis();
		MethodBenchmark mb = this.methodBenchmarks.get(
				"getTriGramsFromDocumentsInCollection"
		);
		mb.increaseCallCount();
		mb.addCallTime(end - start);

		return result;
	}
}
