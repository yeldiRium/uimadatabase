package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.IStringConverter;
import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.implementation.ImplementationMap;

public class DatabaseNameConverter
		implements IStringConverter<Class<? extends Connection>>
{
	@Override
	public Class<? extends Connection> convert(String s)
	{
		return ImplementationMap.implementationMap.getOrDefault(s, null);
	}
}
