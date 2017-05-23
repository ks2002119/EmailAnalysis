package proj.karthik.email.analyzer.core;

import com.typesafe.config.Config;

import org.apache.spark.SparkConf;

/**
 * Sets up custom spark conf for testing.
 */
public class TestModule extends CoreModule {

    public TestModule(Config config, String dataPath, String emailTablePath) {
        super(config, dataPath, emailTablePath);
    }

    @Override
    protected SparkConf getSparkConf() {
        SparkConf sparkConf = super.getSparkConf();
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        return sparkConf;
    }
}
