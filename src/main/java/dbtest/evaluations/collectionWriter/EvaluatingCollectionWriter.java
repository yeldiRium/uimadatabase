package dbtest.evaluations.collectionWriter;

import org.apache.uima.fit.component.JCasConsumer_ImplBase;

import java.util.logging.Logger;

public abstract class EvaluatingCollectionWriter extends JCasConsumer_ImplBase
{
	protected static final Logger logger =
			Logger.getLogger(EvaluatingCollectionWriter.class.getName());
	public static final String PARAM_OUTPUT_FILE = "outputFile";
}
