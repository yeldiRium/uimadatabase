package uimadatabase.dbtest.connection;

import dbtest.connection.Connection;
import dbtest.queryHandler.ElementType;
import dbtest.queryHandler.QueryHandlerInterface;
import dbtest.queryHandler.exceptions.DocumentNotFoundException;
import dbtest.queryHandler.exceptions.QHException;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.cas.CAS;
import org.apache.uima.jcas.JCas;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

public class ConnectionTestCase
{
	class TestConnection extends Connection
	{
		public QueryHandlerInterface injectedQueryHandler;

		@Override
		protected boolean tryToConnect()
		{
			return true;
		}

		@Override
		public void close()
		{

		}

		@Override
		protected void createQueryHandler()
		{
			this.queryHandler = this.injectedQueryHandler;
		}
	}

	@Test
	void Given_ConnectionWithMockedTryToConnectMethod_When_CallingEstablish_Then_CallsTryToConnectUntilItReturnsTrue()
	{
		TestConnection connection = Mockito.mock(TestConnection.class);
		doCallRealMethod().when(connection).establish();
		doCallRealMethod().when(connection).isEstablished();
		doCallRealMethod().when(connection).createQueryHandler();
		// Test that tryToConnect is called four times, the fourth time succeeding.
		when(connection.tryToConnect()).thenReturn(false, false, false, true);
		when(connection.getQueryHandler()).thenReturn(Mockito.mock(QueryHandlerInterface.class));

		connection.establish();

		verify(connection, times(4)).tryToConnect();
		assertEquals(true, connection.isEstablished());
	}

	@Test
	void Given_Connection_When_Established_Then_QueryHandlerShouldBeGettable()
	{
		TestConnection connection = new TestConnection();
		connection.injectedQueryHandler = Mockito.mock(QueryHandlerInterface.class);
		connection.establish();

		assertNotNull(connection.getQueryHandler());
	}

	@Test
	void Given_Connection_When_Established_Then_SetUpDatabaseShouldBeCalledOnQueryHandler() throws IOException
	{
		TestConnection connection = new TestConnection();
		connection.injectedQueryHandler = Mockito.mock(QueryHandlerInterface.class);
		connection.establish();

		verify(connection.injectedQueryHandler, times(1)).setUpDatabase();
	}
}
