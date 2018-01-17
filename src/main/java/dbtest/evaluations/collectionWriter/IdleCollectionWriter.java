package dbtest.evaluations.collectionWriter;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.jcas.JCas;

/**
 * The IdleCollectionWriter does nothing.
 * It is just there for cases where we only want a reader but UIMA expects an
 * AnalysisEngine anyway.
 */
public class IdleCollectionWriter extends JCasConsumer_ImplBase
{
	@Override
	public void process(JCas aJCas) throws AnalysisEngineProcessException
	{

	}
}
