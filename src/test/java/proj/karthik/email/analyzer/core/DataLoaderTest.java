package proj.karthik.email.analyzer.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import proj.karthik.email.analyzer.SparkTest;
import proj.karthik.email.analyzer.util.SchemaUtil;
import proj.karthik.email.analyzer.report.spark.functions.FieldExploder;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link EmailParserImpl}
 */
public class DataLoaderTest extends SparkTest {

    @Test
    public void testDataLoader() throws Exception {
        DataLoader dataLoader = injector.getInstance(DataLoader.class);
        dataLoader.load(dataPath, emailTablePath);
        Dataset<Row> jsonDataset = sqlContext.read()
                .option("inferSchema", "true")
                .json(emailTablePath);
        long count = jsonDataset.count();
        assertEquals(6, count);
    }

    @Test
    public void testMessageTypeTable() throws Exception {
        simpleVolume1 = simpleVolume1.withColumn("received_tstamp", simpleVolume1.col("header.timestamp")
                .cast("timestamp"))
                .withColumn("sent_tstamp", simpleVolume1.col("body.sent_at")
                .cast("timestamp"));
        JavaRDD<Row> rdd = simpleVolume1.select(
                simpleVolume1.col("sent_tstamp"),
                simpleVolume1.col("received_tstamp"),
                simpleVolume1.col("header"))
                .javaRDD().flatMap(new FieldExploder());
        assertEquals(75, rdd.count());
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rdd, SchemaUtil.getExplodedTableSchema());
        assertEquals(75, dataFrame.count());
    }
}
