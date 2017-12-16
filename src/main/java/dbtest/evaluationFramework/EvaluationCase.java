package dbtest.evaluationFramework;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;

public interface EvaluationCase
{
	ConnectionRequest requestConnection();

	void run(
			ResourceProvider resourceProvider,
			OutputService outputService,
			ConnectionResponse connectionResponse
	);
}
