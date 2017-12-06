package org.hucompute.services.uima.database.xmi;

import org.apache.commons.lang.time.StopWatch;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.TypeCapability;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;

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
public class POSCounter
extends JCasConsumer_ImplBase
{

	private StopWatch stopWatch;
	
	TObjectIntHashMap<String> counts = new TObjectIntHashMap<>();

	@Override
	public void initialize(UimaContext context)
			throws ResourceInitializationException {
		super.initialize(context);
		stopWatch = new StopWatch();
		stopWatch.start();
		stopWatch.suspend();
	}

	int processed;
	@Override
	public void process(JCas aJCas) throws AnalysisEngineProcessException {
		for (Lemma pos: JCasUtil.select(aJCas, Lemma.class)) {
			counts.adjustOrPutValue(pos.getValue(), 1, 1);
		}
		if(processed++ % 100 == 0)
			System.out.println(processed);
	}
	
	@Override
	public void collectionProcessComplete() throws AnalysisEngineProcessException {
		super.collectionProcessComplete();
		counts.forEachEntry(new TObjectIntProcedure<String>() {
			@Override
			public boolean execute(String a, int b) {
				return true;
			}
		});
	}

}
