package org.hucompute.services.uima.eval.main;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.hucompute.services.uima.eval.main.CLIArguments.EvaluateCommand;
import org.hucompute.services.uima.eval.main.CLIArguments.VisualizeCommand;

public class Main
{
	private final EvaluateCommand evaluateCommand = new EvaluateCommand();
	private final VisualizeCommand visualizeCommand = new VisualizeCommand();
	private JCommander jCommander;

	public static void main(String[] args)
	{
		Main main = new Main();
		main.handleInputArguments(args);
		//main.run();
	}

	void handleInputArguments(String[] args)
	{
		this.jCommander = JCommander.newBuilder()
				.addCommand("evaluate", this.evaluateCommand)
				.addCommand("visualize", this.visualizeCommand)
				.build();

		try
		{
			jCommander.parse(args);
		} catch (ParameterException e)
		{
			System.out.println(e.getMessage());
			this.showUsage();
		}

		if (this.evaluateCommand.help)
		{
			this.showUsage();
		}
	}

	void showUsage()
	{
		this.jCommander.usage();
		System.exit(0);
	}
}
