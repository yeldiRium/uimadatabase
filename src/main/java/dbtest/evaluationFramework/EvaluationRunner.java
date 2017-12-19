package dbtest.evaluationFramework;

import dbtest.connection.ConnectionManager;
import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.exceptions.EvaluationFailedRerunnableException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.concurrent.ExecutionException;

public class EvaluationRunner implements Runnable
{
	protected Configuration configuration;
	protected ConnectionManager connectionManager;

	public EvaluationRunner(InputStream configFile, ConnectionManager connectionManager)
	{
		this.loadConfig(configFile);
		this.connectionManager = connectionManager;
	}

	protected void loadConfig(InputStream configFile)
	{
		Constructor constructor = new Constructor(Configuration.class);
		Yaml yaml = new Yaml(constructor);
		this.configuration = yaml.load(configFile);
	}

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
