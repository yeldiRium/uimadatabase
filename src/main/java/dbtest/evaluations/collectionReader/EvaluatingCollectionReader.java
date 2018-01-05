package dbtest.evaluations.collectionReader;

import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;

import java.io.File;
import java.util.logging.Logger;

public abstract class EvaluatingCollectionReader extends CasCollectionReader_ImplBase
{
	protected static final Logger logger =
			Logger.getLogger(EvaluatingCollectionReader.class.getName());

	// UIMA Parameters
	public static final String PARAM_OUTPUT_FILE = "outputFile";

	@ConfigurationParameter(name = PARAM_OUTPUT_FILE, mandatory = false)
	public File outputFile;
}
