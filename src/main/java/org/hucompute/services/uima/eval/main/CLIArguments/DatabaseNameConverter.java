package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.IStringConverter;
import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.database.connection.Connections;

public class DatabaseNameConverter
		implements IStringConverter<Class<? extends Connection>>
{
	@Override
	public Class<? extends Connection> convert(String s)
	{
		// No error checking necessary, since validators are run beforehand.
		return Connections.getConnectionClassForName(s);
	}
}
