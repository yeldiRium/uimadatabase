package org.hucompute.services.uima.eval.main;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.hucompute.services.uima.eval.main.CLIArguments.CLIArguments;

public class Main
{
	private final CLIArguments arguments = new CLIArguments();
	private JCommander jCommander;

	public static void main(String[] args) {
		Main main = new Main();
		main.handleInputArguments(args);
		//main.run();
	}

	void handleInputArguments(String[] args) {
		this.jCommander = new JCommander(this.arguments);

		try
		{
			jCommander.parse(args);
		} catch (ParameterException e) {
			System.out.println(e.getMessage());
			this.showUsage();
		}

		System.out.println(this.arguments.dbs);
		System.out.println(this.arguments.evaluations);

		if (this.arguments.help) {
			this.showUsage();
		}
	}

	void showUsage() {
		this.jCommander.usage();
		System.exit(0);
	}
}
