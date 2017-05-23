package proj.karthik.email.analyzer.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import com.typesafe.config.Config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

import proj.karthik.email.analyzer.report.ReportFactory;
import proj.karthik.email.analyzer.report.ReportFactoryImpl;
import proj.karthik.email.analyzer.util.ReportUtil;
import proj.karthik.email.analyzer.util.DatasetUtil;


/**
 * Core dependencies are wired up here.
 */
public class CoreModule extends AbstractModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreModule.class);

    public static final String DATA = "data";
    public static final String EMAIL_TABLE_PATH = "email_table_path";

    private static final String SPARK_CONFS = "spark.confs";
    private static final Pattern PATTERN_EQUALS = Pattern.compile("=");
    private static final String LOCAL_MASTER = "local[*]";

    private final Config config;
    private final String dataPath;
    private final String emailTablePath;

    public CoreModule(Config config, String dataPath, String emailTablePath) {
        this.config = config;
        this.dataPath = dataPath;
        this.emailTablePath = emailTablePath;
    }

    @Override
    protected void configure() {
        bind(Config.class).toInstance(config);
        bind(String.class).annotatedWith(Names.named(DATA)).toInstance(dataPath);
        bind(String.class).annotatedWith(Names.named(EMAIL_TABLE_PATH)).toInstance(emailTablePath);
        install(new FactoryModuleBuilder()
                .implement(new TypeLiteral<EmailParser>(){}, EmailParserImpl.class)
                .build(EmailParserFactory.class));
        setupJsonMapper();
        setupSparkContext();
        bind(DataLoader.class).asEagerSingleton();
        bind(ReportUtil.class).asEagerSingleton();
        bind(DatasetUtil.class).asEagerSingleton();
        bind(ReportFactory.class).to(ReportFactoryImpl.class).asEagerSingleton();
    }

    //---------------------------- protected methods --------------------------------------------//

    protected void setupJsonMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.CLOSE_CLOSEABLE);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        bind(ObjectMapper.class).toInstance(objectMapper);
    }

    protected void setupSparkContext() {
        SparkConf sparkConf = getSparkConf();
        if (config.hasPath(SPARK_CONFS)) {
            config.getStringList(SPARK_CONFS).stream()
                    .forEach(conf -> {
                        String[] confEntry = PATTERN_EQUALS.split(conf);
                        if (confEntry.length == 2) {
                            LOGGER.debug("Setting spark conf: {} = {}", confEntry[0], confEntry[1]);
                            sparkConf.set(confEntry[0], confEntry[1]);
                        }
                    });
        }
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        bind(JavaSparkContext.class).toInstance(javaSparkContext);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        bind(SQLContext.class).toInstance(sqlContext);
    }

    protected SparkConf getSparkConf() {
        return new SparkConf()
                    .setAppName("EmailAnalyzer")
                    .setMaster(LOCAL_MASTER);
    }
}
