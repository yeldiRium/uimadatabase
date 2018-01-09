package dbtest.queryHandler;

import dbtest.queryHandler.exceptions.DocumentNotFoundException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractQueryHandler implements QueryHandlerInterface
{
	@Override
	public double calculateInverseDocumentFrequency(String lemma)
	{
		return Math.log((countElementsOfType(ElementType.Document) /
				(double) countDocumentsContainingLemma(lemma))
		);
	}

	@Override
	public Map<String, Double>
	calculateInverseDocumentFrequenciesForLemmataInDocument(String documentId)
			throws DocumentNotFoundException
	{
		double docCount = (double) countElementsOfType(ElementType.Document);
		Map<String, Double> inverseDocumentFrequencies =
				new ConcurrentHashMap<>();
		Set<String> lemmata = getLemmataForDocument(documentId);
		Map<String, Integer> lemmaOccurenceCount =
				this.countOccurencesForEachLemmaInAllDocuments();

		lemmata.parallelStream().forEach(e -> {
			inverseDocumentFrequencies.put(
					e.replaceAll("\"", ""),
					Math.log(docCount /
							((double) lemmaOccurenceCount.getOrDefault(e, 0))
					)
			);
		});
		return inverseDocumentFrequencies;
	}

	@Override
	public double calculateTFIDFForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException
	{
		return this.calculateTermFrequencyWithLogNormForLemmaInDocument(
				lemma, documentId
		) * this.calculateInverseDocumentFrequency(lemma);
	}

	@Override
	public Map<String, Double> calculateTFIDFForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException
	{
		Map<String, Double> tf =
				calculateTermFrequenciesForLemmataInDocument(documentId);
		Map<String, Double> idf =
				calculateInverseDocumentFrequenciesForLemmataInDocument(
						documentId
				);
		Map<String, Double> tfidf = new ConcurrentHashMap<>();
		tf.entrySet().parallelStream().forEach(
				e -> tfidf.put(
						e.getKey(),
						e.getValue() * idf.get(e.getKey())
				)
		);
		return tfidf;
	}

	@Override
	public Map<String, Map<String, Double>>
	calculateTFIDFForLemmataInAllDocuments()
	{
		HashMap<String, Map<String, Double>> tfidfs = new HashMap<>();
		this.getDocumentIds().forEach(documentId -> {
			tfidfs.put(
					documentId,
					this.calculateTFIDFForLemmataInDocument(documentId)
			);
		});
		return tfidfs;
	}
}
