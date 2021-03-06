package org.hucompute.services.uima.eval.main;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.hucompute.services.uima.eval.main.CLIArguments.EvaluateCommand;
import org.hucompute.services.uima.eval.main.CLIArguments.VisualizeCommand;
import org.hucompute.services.uima.eval.utility.logging.PlainFormatter;

import java.io.IOException;
import java.util.logging.*;

public class Main
{
	private final EvaluateCommand evaluateCommand = new EvaluateCommand();
	private final VisualizeCommand visualizeCommand = new VisualizeCommand();
	private JCommander jCommander;

	public static void main(String[] args) throws IOException
	{
		// Clean up logging to stdout
		Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(Level.INFO);
		Formatter plainFormatter = new PlainFormatter();
		for (Handler h : rootLogger.getHandlers())
		{
			h.setFormatter(plainFormatter);
			h.setLevel(Level.INFO);
		}

		Main main = new Main();
		main.handleInputArguments(args);
		main.run();
	}

	void handleInputArguments(String[] args)
	{
		this.jCommander = JCommander.newBuilder()
				.addCommand("evaluate", this.evaluateCommand)
				.addCommand("visualize", this.visualizeCommand)
				.build();

		try
		{
			this.jCommander.parse(args);
		} catch (ParameterException e)
		{
			System.out.println(e.getMessage());
			this.jCommander.usage();
			System.exit(1);
		}

		if (this.jCommander.getParsedCommand() == null
				|| this.evaluateCommand.help
				|| this.visualizeCommand.help)
		{
			this.jCommander.usage();
			System.exit(0);
		}
	}

	private void run() throws IOException
	{
		if (this.jCommander.getParsedCommand().equals("evaluate"))
		{
			EvaluationPipeline.run(
					this.evaluateCommand.evaluations,
					this.evaluateCommand.dbs
			);
		}

		if (this.jCommander.getParsedCommand().equals("visualize"))
		{
			VisualizationPipeline.main(new String[]{});
		}
	}
}
