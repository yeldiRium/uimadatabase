package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.hucompute.services.uima.eval.database.connection.Connection;
import org.hucompute.services.uima.eval.evaluation.framework.EvaluationCase;

import java.util.ArrayList;
import java.util.List;

@Parameters(commandDescription = "Run evaluations on databases. Note that the" +
		" ReadEvaluation has to be run before anything else, so that content " +
		"exists in the database.")
public class EvaluateCommand
{
	@Parameter(
			names = {"-h", "--help"},
			help = true,
			description = "Show help text."
	)
	public boolean help = false;

	@Parameter(
			required = true,
			names = {"-e", "--evaluations"},
			description = "Comma-separated list of evaluations to be run.",
			validateWith = EvaluationNameValidator.class,
			converter = EvaluationNameConverter.class
	)
	public List<EvaluationCase> evaluations = new ArrayList<>();

	@Parameter(
			required = true,
			names = {"-d", "--dbs", "--databases"},
			description = "Comma-separated list of databases to run the evaluations on.",
			validateWith = DatabaseNameValidator.class,
			converter = DatabaseNameConverter.class
	)
	public List<Class<? extends Connection>> dbs = new ArrayList<>();
}
