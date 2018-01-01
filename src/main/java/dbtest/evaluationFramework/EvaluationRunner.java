package dbtest.evaluationFramework;

import dbtest.connection.ConnectionManager;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.exceptions.EvaluationFailedRerunnableException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

public class EvaluationRunner implements Runnable
{
	protected Configuration configuration;
	protected ConnectionManager connectionManager;

	public EvaluationRunner(InputStream configFile, ConnectionManager connectionManager) throws IOException
	{
		this.loadConfig(configFile);
		this.connectionManager = connectionManager;
	}

	/**
	 * Loads the yaml config.
	 * This instantiates all EvaluationCases and the OutputProvider automatically.
	 * @param configFile
	 */
	protected void loadConfig(InputStream configFile) throws IOException
	{
		Constructor constructor = new Constructor(Configuration.class);
		Yaml yaml = new Yaml(constructor);
		this.configuration = yaml.load(configFile);
		this.configuration.getOutputProvider().configurePath(
				System.getenv("OUTPUT_DIR")
		);
	}

	/**
	 * Executes all configured EvaluationCases in order.
	 * Passes each its according connectionResponse once it is ready and an
	 * OutputProvider instance for logging.
	 */
	@Override
	public void run()
	{
		for (EvaluationCase evaluationCase : this.configuration.getEvaluations())
		{
			ConnectionRequest connectionRequest = evaluationCase.requestConnection();
			ConnectionResponse connectionResponse = null;
			try
			{
				connectionResponse = this.connectionManager.submitRequest(connectionRequest).get();
			} catch (InterruptedException | ExecutionException e)
			{
				Thread.currentThread().interrupt();
			}
			System.out.println("Running EvaluationCase " + evaluationCase.getClass().getName());
			boolean success = false;
			while (!success)
			{
				try
				{
					evaluationCase.run(
							connectionResponse,
							this.configuration.getOutputProvider()
					);
					success = true;
				} catch (EvaluationFailedRerunnableException e)
				{
					System.out.println("Evaluation failed. Rerunning...");
				}
			}
		}
	}
}
