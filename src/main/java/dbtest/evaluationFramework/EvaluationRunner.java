package dbtest.evaluationFramework;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;

public class EvaluationRunner implements Runnable
{
	protected Configuration configuration;

	public EvaluationRunner(InputStream configFile) {
		this.loadConfig(configFile);
	}

	protected void loadConfig(InputStream configFile) {
		Constructor constructor = new Constructor(Configuration.class);
		Yaml yaml = new Yaml(constructor);
		this.configuration = yaml.load(configFile);
	}

	@Override
	public void run()
	{

	}
}
