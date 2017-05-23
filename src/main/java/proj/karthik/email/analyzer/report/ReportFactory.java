package proj.karthik.email.analyzer.report;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Factory that helps creating reports.
 */
@FunctionalInterface
public interface ReportFactory {

    /**
     * Generates report for the given prebuilt report type.
     *
     * @param prebuildReportType
     * @param dataset
     * @param exploded
     * @return result
     */
    Dataset<Row> generate(PrebuildReportType prebuildReportType, Dataset<Row> dataset,
            Dataset<Row> exploded);
}
