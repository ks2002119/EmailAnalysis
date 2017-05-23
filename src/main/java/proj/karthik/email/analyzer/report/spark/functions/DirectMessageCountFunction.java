package proj.karthik.email.analyzer.report.spark.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import proj.karthik.email.analyzer.util.SchemaUtil;

import static scala.collection.JavaConversions.seqAsJavaList;

/**
 * Created by ksubramanian on 3/3/17.
 */
public class DirectMessageCountFunction implements FlatMapFunction<Row, Row> {

    private transient StructType directMessageCountSchema;

    public DirectMessageCountFunction() {
        directMessageCountSchema = SchemaUtil.getDirectMessageCountSchema();
    }


    @Override
    public Iterator<Row> call(final Row row) throws Exception {
        // Get the header for the email
        List<String> toEmails = seqAsJavaList(row.getSeq(0));
        List<String> ccEmails = seqAsJavaList(row.getSeq(1));
        List<String> bccEmails = seqAsJavaList(row.getSeq(2));
        if (toEmails.size() == 1 && ccEmails.isEmpty() && bccEmails.isEmpty()) {
            // Direct message
            return Arrays.<Row>asList(new GenericRowWithSchema(new Object[]{toEmails.get(0), 1l},
                    directMessageCountSchema)).iterator();
        } else {
            // Indirect message
            return Collections.<Row>emptyList().iterator();
        }
    }
}
