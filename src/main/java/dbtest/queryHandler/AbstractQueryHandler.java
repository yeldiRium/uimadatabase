package dbtest.queryHandler;

import dbtest.evaluations.collectionReader.EvaluatingCollectionReader;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.TypeNotCountableException;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.jcas.JCas;

import javax.naming.OperationNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public abstract class AbstractQueryHandler implements QueryHandlerInterface
{
	protected static final Logger logger =
			Logger.getLogger(AbstractQueryHandler.class.getName());

	/**
	 * @param paragraph The Paragraph.
	 * @param document  The document in which the paragraph occurs.
	 */
	@Override
	public void storeParagraph(Paragraph paragraph, JCas document)
	{
		this.storeParagraph(paragraph, document, null);
	}

	/**
	 * @param sentence  The Sentence.
	 * @param document  The Document in which the entence occurs.
	 * @param paragraph The Paragraph, in which the Sentence occurs.
	 */
	@Override
	public void storeSentence(
			Sentence sentence,
			JCas document,
			Paragraph paragraph
	)
	{
		this.storeSentence(sentence, document, paragraph, null);
	}

	/**
	 * @param token     The Token.
	 * @param document  The id of the document in which the Token occurs.
	 * @param paragraph The paragraph, in which the Token occurs.
	 * @param sentence  The sentence, in which the Token occurs.
	 */
	@Override
	public void storeToken(
			Token token,
			JCas document,
			Paragraph paragraph,
			Sentence sentence
	)
	{
		storeToken(token, document, paragraph, sentence, null);
	}

	@Override
	public double calculateInverseDocumentFrequency(String lemma)
			throws OperationNotSupportedException
	{
		try
		{
			return Math.log((countElementsOfType(ElementType.Document) /
					(double) countDocumentsContainingLemma(lemma))
			);
		} catch (TypeNotCountableException e)
		{
			throw new OperationNotSupportedException();
		}
	}

	@Override
	public Map<String, Double>
	calculateInverseDocumentFrequenciesForLemmataInDocument(String documentId)
			throws DocumentNotFoundException, OperationNotSupportedException
	{
		try
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
		} catch (TypeNotCountableException e)
		{
			throw new OperationNotSupportedException();
		}
	}

	@Override
	public double calculateTFIDFForLemmaInDocument(
			String lemma,
			String documentId
	) throws DocumentNotFoundException, OperationNotSupportedException
	{
		try
		{
			return this.calculateTermFrequencyWithLogNormForLemmaInDocument(
					lemma, documentId
			) * this.calculateInverseDocumentFrequency(lemma);
		} catch (OperationNotSupportedException e)
		{
			throw new OperationNotSupportedException();
		}
	}

	@Override
	public Map<String, Double> calculateTFIDFForLemmataInDocument(
			String documentId
	) throws DocumentNotFoundException, OperationNotSupportedException
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
			throws OperationNotSupportedException
	{
		HashMap<String, Map<String, Double>> tfidfs = new HashMap<>();
		for(String documentId : this.getDocumentIds())
		{
			try
			{
				tfidfs.put(
						documentId,
						this.calculateTFIDFForLemmataInDocument(documentId)
				);
			} catch (DocumentNotFoundException e)
			{
				logger.warning("DocumentId " + documentId + " could " +
						"not be found in the database, although it was " +
						"there just a moment ago. Please check for " +
						"concurrent access.");
			}
		};
		return tfidfs;
	}
}
