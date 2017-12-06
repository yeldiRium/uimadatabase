package dbtest;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.TypeCapability;
import org.apache.uima.jcas.JCas;

/**
 * DKPro Annotator for the MateToolsMorphTagger.
 */
@TypeCapability(
		inputs = {
				"de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token",
				"de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence",
				"de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma"
		},
		outputs = {
				"de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.Morpheme"
		}
		)
public class StatusPrinter
extends JCasConsumer_ImplBase
{


	int processed;
	@Override
	public void process(JCas aJCas) throws AnalysisEngineProcessException {
	}
	
	@Override
	public void collectionProcessComplete() throws AnalysisEngineProcessException {
		super.collectionProcessComplete();
	}

}
