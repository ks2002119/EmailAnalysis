package proj.karthik.email.analyzer.report.spark.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.util.Date;
import java.util.List;

import proj.karthik.email.analyzer.util.SchemaUtil;

import static scala.collection.JavaConversions.seqAsJavaList;

/**
 * Computes the received message count from an email document.
 * (Total number of to, cc and bcc
 */
public class ReceiveCountFunction implements Function<Row, Row> {

    private transient StructType receiveCountSchema;

    public ReceiveCountFunction() {
        receiveCountSchema = SchemaUtil.getReceiveCountSchema();
    }


    @Override
    public Row call(final Row row) throws Exception {
        // Get the header for the email
        Date date = row.getDate(0);
        List<String> toEmails = seqAsJavaList(row.getSeq(1));
        List<String> ccEmails = seqAsJavaList(row.getSeq(2));
        List<String> bccEmails = seqAsJavaList(row.getSeq(3));
        long count = toEmails.size() + ccEmails.size() + bccEmails.size();
        return new GenericRowWithSchema(new Object[]{date, count}, receiveCountSchema);
    }
}
