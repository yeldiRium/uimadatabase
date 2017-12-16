package dbtest.evaluationFramework;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;

import java.io.OutputStream;

public interface EvaluationCase
{
	ConnectionRequest requestConnection();

	void run(
			OutputStream outputStream,
			ConnectionResponse connectionResponse
	);
}
