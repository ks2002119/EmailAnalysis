package proj.karthik.email.analyzer;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;

import proj.karthik.email.analyzer.core.CoreModule;
import proj.karthik.email.analyzer.report.ReportFactory;
import proj.karthik.email.analyzer.report.ReportFactoryImplTest;
import proj.karthik.email.analyzer.util.DatasetUtil;

/**
 * Base class for spark based testing.
 */
public class SparkTest {

    protected static SQLContext sqlContext;
    protected static Dataset<Row> largeDirect, largeIndirect, fastResponse,
            simpleVolume1, bccTable;
    protected static Injector injector;
    protected static String emailTablePath;
    protected static String dataPath;
    protected static ReportFactory reportFactory;
    protected static DatasetUtil datasetUtil;
    protected static Dataset<Row> domainTable;
    protected static Dataset<Row> languageTable;
    protected static Dataset<Row> connectionsTable;

    @BeforeClass
    public static void setUp() throws Exception {
        injector = TestConfUtil.getInjector();
        sqlContext = injector.getInstance(SQLContext.class);
        dataPath = TestConfUtil.getInjector().getInstance(
                Key.get(String.class, Names.named(CoreModule.DATA)));
        emailTablePath = TestConfUtil.getInjector().getInstance(
                Key.get(String.class, Names.named(CoreModule.EMAIL_TABLE_PATH)));
        datasetUtil = injector.getInstance(DatasetUtil.class);
        Paths.get(emailTablePath).toFile().deleteOnExit();

        largeDirect = createDataset("/large_direct_table.json");
        largeIndirect = createDataset("/large_indirect_table.json");
        fastResponse = createDataset("/fast_reponse_table.json");
        simpleVolume1 = createDataset("/simple_volume_table1.json");
        bccTable = createDataset("/simple_bcc_table1.json");
        domainTable = createDataset("/simple_domain_table1.json");
        languageTable = createDataset("/simple_lang_table1.json");
        connectionsTable = createDataset("/simple_connections_table.json");

        reportFactory = injector.getInstance(ReportFactory.class);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Files.deleteIfExists(Paths.get(emailTablePath));
    }

    private static Dataset<Row> createDataset(String path) throws URISyntaxException {
        URL url = ReportFactoryImplTest.class.getResource(path);
        File file = new File(url.toURI());
        Dataset<Row> dataset = sqlContext.read()
                .schema((StructType) StructType.fromJson(datasetUtil.getDefaultSchema()))
                .json(file.getAbsolutePath().toString());
        return datasetUtil.explodeDateTime(dataset);
    }

    protected Dataset<Row> explode(Dataset<Row> dataset) {
        return datasetUtil.explode(dataset);
    }

    protected Date dateOf(final String date) {
        return Date.valueOf(date);
    }

    protected Row rowOf(final Object[] objects) {
        return new GenericRow(objects);
    }
}
