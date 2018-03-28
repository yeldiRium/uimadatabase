package org.hucompute.services.uima.eval.main.CLIArguments;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

public class CLIArguments
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
			converter = EvaluationConverter.class
	)
	public List<String> evaluations = new ArrayList<>();

	@Parameter(
			required = true,
			names = {"-d", "--dbs", "--databases"},
			description = "Comma-separated list of databases to run the evaluations on.",
			validateWith = DatabaseNameValidator.class,
			converter = DatabaseNameConverter.class
	)
	public List<String> dbs = new ArrayList<>();
}
