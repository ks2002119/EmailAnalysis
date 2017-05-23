package proj.karthik.email.analyzer.util;

import com.google.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import proj.karthik.email.analyzer.Bootstrap;
import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.report.spark.functions.FieldExploder;

/**
 * Utility class for datasets.
 */
public class DatasetUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetUtil.class);

    private final SQLContext sqlContext;
    private final String defaultSchema;

    @Inject
    public DatasetUtil(final SQLContext sqlContext) {
        this.sqlContext = sqlContext;
        LOGGER.info("Loading schema file");
        defaultSchema = loadDefaultSchema();
    }

    /**
     * Explode the datatime fields with the base dataset.
     *
     * @param dataset
     * @return exploded
     */
    public Dataset<Row> explodeDateTime(Dataset<Row> dataset) {
        // Form 2 entries - date and timestamp from single timestamp string.
        // Date is useful for calculating things on a daily basis.
        // Timestamp allows more flexibility.
        dataset= dataset.withColumn("date",
                dataset.col("header.timestamp").cast("date"))
                .withColumn("timestamp", dataset.col("header.timestamp").cast("timestamp"));
        return dataset;
    }

    /**
     * Explode the given base dataset
     * @param baseDataset
     * @return exploded
     */
    public Dataset<Row> explode(Dataset<Row> baseDataset) {
        baseDataset = baseDataset.withColumnRenamed("timestamp", "received_tstamp")
                .withColumn("sent_tstamp", baseDataset.col("body.sent_at")
                        .cast("timestamp"));
        JavaRDD<Row> rdd = baseDataset.select(
                    baseDataset.col("sent_tstamp"),
                    baseDataset.col("received_tstamp"),
                    baseDataset.col("header"))
                .javaRDD()
                .flatMap(new FieldExploder());
        return sqlContext.createDataFrame(rdd, SchemaUtil.getExplodedTableSchema());
    }

    /**
     * Returns the default schema for the dataset.
     *
     * @return defaultSchema
     */
    public String getDefaultSchema() {
        return defaultSchema;
    }

    private static String loadDefaultSchema() {
        try (InputStream inputStream = Bootstrap.class.getResourceAsStream("/schema.json")) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, byteArrayOutputStream);
            byteArrayOutputStream.flush();
            byteArrayOutputStream.close();
            return byteArrayOutputStream.toString();
        } catch (IOException e) {
            throw new AnalyzerException(e, "Unable to load table schema");
        }
    }
}
