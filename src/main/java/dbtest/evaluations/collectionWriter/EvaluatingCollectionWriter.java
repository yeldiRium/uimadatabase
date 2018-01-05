package dbtest.evaluations.collectionWriter;

import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;

import java.io.File;
import java.util.logging.Logger;

public abstract class EvaluatingCollectionWriter extends JCasConsumer_ImplBase
{
	protected static final Logger logger =
			Logger.getLogger(EvaluatingCollectionWriter.class.getName());

	// UIMA Parameters
	public static final String PARAM_OUTPUT_FILE = "outputFile";

	@ConfigurationParameter(name = PARAM_OUTPUT_FILE, mandatory = false)
	public File outputFile;
}
