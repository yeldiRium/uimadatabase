package dbtest.evaluations.collectionReader;

import org.apache.uima.fit.component.CasCollectionReader_ImplBase;

import java.util.logging.Logger;

public abstract class EvaluatingCollectionReader extends CasCollectionReader_ImplBase
{
	protected static final Logger logger =
			Logger.getLogger(EvaluatingCollectionReader.class.getName());
	public static final String PARAM_OUTPUT_FILE = "outputFile";
}
