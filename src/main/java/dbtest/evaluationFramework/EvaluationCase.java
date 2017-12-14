package dbtest.evaluationFramework;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;

import java.util.logging.Logger;

public interface EvaluationCase
{
	ConnectionRequest requestConnection();

	void run(
			ResourceProvider resourceProvider,
			Logger logger,
			ConnectionResponse connectionResponse
	);
}
