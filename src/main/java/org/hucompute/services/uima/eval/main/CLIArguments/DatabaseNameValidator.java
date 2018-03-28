package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import org.hucompute.services.uima.eval.database.connection.implementation.ImplementationMap;

public class DatabaseNameValidator implements IParameterValidator
{
	@Override
	public void validate(String name, String databases) throws ParameterException
	{
		String[] databaseNames = databases.split(",");
		for (int i = 0; i < databaseNames.length; i++)
		{
			String databaseName = databaseNames[i];
			if (!ImplementationMap.implementationMap.containsKey(databaseName))
			{
				this.wrongDatabaseName(databaseName);
			}
			for (int j = 0; i < databaseNames.length; i++)
			{
				if (i == j)
				{
					continue;
				}
				if (databaseNames[i].equals(databaseNames[j]))
				{
					throw new ParameterException("The database name \"" +
							databaseName + "\" is duplicated. Please specify " +
							"each database only once.");
				}
			}
		}
	}

	private void wrongDatabaseName(String databaseName)
	{
		String availableDatabaseNames = String.join(
				", ",
				ImplementationMap.implementationMap.keySet()
		);
		throw new ParameterException("The database name \"" + databaseName +
				"\" is not valid. Available values are: " +
				availableDatabaseNames);
	}
}
