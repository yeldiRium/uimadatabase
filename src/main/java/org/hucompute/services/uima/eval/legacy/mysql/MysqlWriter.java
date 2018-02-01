package org.hucompute.services.uima.eval.legacy.mysql;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.eval.legacy.AbstractWriter;

public class MysqlWriter extends AbstractWriter
{

	@Override
	public void initialize(UimaContext context)
			throws ResourceInitializationException
	{
		super.initialize(context);
		//Initialize db connection
	}

	@Override
	public void process(JCas jCas) throws AnalysisEngineProcessException
	{
		resumeWatch();

		//saveCAS(jCas)

		suspendWatch();
		log();
	}
}