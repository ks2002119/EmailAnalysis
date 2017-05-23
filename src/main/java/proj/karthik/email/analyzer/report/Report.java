package proj.karthik.email.analyzer.report;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Interface that represents a report.
 */
@FunctionalInterface
public interface Report {

    /**
     * Run the report for the given input.
     *
     * @param original
     * @param exploded
     * @return report
     */
    Dataset<Row> runReport(Dataset<Row> original, Dataset<Row> exploded);
}
