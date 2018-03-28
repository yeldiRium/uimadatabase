package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import org.hucompute.services.uima.eval.database.connection.Connections;

public class DatabaseNameValidator implements IParameterValidator
{
	@Override
	public void validate(String name, String databases)
			throws ParameterException
	{
		String[] databaseNames = databases.split(",");
		for (int i = 0; i < databaseNames.length; i++)
		{
			String databaseName = databaseNames[i];
			if (!Connections.names().contains(databaseName))
			{
				this.wrongDatabaseName(databaseName);
			}

			for (int j = 0; j < databaseNames.length; j++)
			{
				if (i != j && databaseNames[i].equals(databaseNames[j]))
				{
					throw new ParameterException("The database name \"" +
							databaseName + "\" is duplicated. Please specify " +
							"each database only once.");
				}
			}
		}
	}

	private void wrongDatabaseName(String databaseName)
			throws ParameterException
	{
		String availableDatabaseNames = String.join(
				", ",
				Connections.names()
		);
		throw new ParameterException("The database name \"" + databaseName +
				"\" is not valid. Available values are: " +
				availableDatabaseNames);
	}
}
