package proj.karthik.email.analyzer.report.spark.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import java.sql.Timestamp;

import proj.karthik.email.analyzer.util.SchemaUtil;

/**
 * Computes timestamp difference.
 */
public class TimediffFunction implements Function<Row, Row> {

    @Override
    public Row call(final Row row) throws Exception {
        Timestamp receivedDate = row.getTimestamp(row.fieldIndex("tstamp"));
        Timestamp emailDate = row.getTimestamp(row.fieldIndex("origin_tstamp"));
        if (receivedDate != null && emailDate != null) {
            return new GenericRowWithSchema(new Object[]{
                    row.getString(row.fieldIndex("id")),
                    row.getString(row.fieldIndex("subject")),
                    Long.valueOf(receivedDate.getTime() - emailDate.getTime())
            }, SchemaUtil.getTimeDiffSchema());
        } else {
            return new GenericRowWithSchema(new Object[]{
                    row.getString(row.fieldIndex("id")),
                    row.getString(row.fieldIndex("subject")), Long.MAX_VALUE
            }, SchemaUtil.getTimeDiffSchema());
        }
    }
}
