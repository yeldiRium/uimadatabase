package dbtest.evaluationFramework;

import dbtest.connection.ConnectionRequest;
import dbtest.connection.ConnectionResponse;
import dbtest.evaluationFramework.exceptions.EvaluationFailedRerunnableException;

public interface EvaluationCase
{
	ConnectionRequest requestConnection();

	/**
	 * @param connectionResponse Contains all Connections requested in
	 *                           #requestConnection().
	 * @throws EvaluationFailedRerunnableException when a failing action can be
	 *                                             fixed by rerunning the
	 *                                             EvaluationCase.
	 */
	void run(
			ConnectionResponse connectionResponse,
			OutputProvider outputProvider
	) throws EvaluationFailedRerunnableException;
}
