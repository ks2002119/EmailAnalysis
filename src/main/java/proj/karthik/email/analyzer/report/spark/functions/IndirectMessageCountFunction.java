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
 * Computes indirect message count
 */
public class IndirectMessageCountFunction implements FlatMapFunction<Row, Row> {


    private transient StructType indirectMessageCountSchema;

    public IndirectMessageCountFunction() {
        indirectMessageCountSchema = SchemaUtil.getIndirectMessageCountSchema();
    }


    @Override
    public Iterator<Row> call(final Row row) throws Exception {
        // Get the header for the email
        String from = row.getString(0);
        List<String> toEmails = seqAsJavaList(row.getSeq(1));
        List<String> ccEmails = seqAsJavaList(row.getSeq(2));
        List<String> bccEmails = seqAsJavaList(row.getSeq(3));
        if (toEmails.size() == 1 && ccEmails.isEmpty() && bccEmails.isEmpty()) {
            // Direct message
            return Collections.<Row>emptyList().iterator();
        } else {
            // Indirect message
            long count = toEmails.size() + ccEmails.size() + bccEmails.size();
            return Arrays.<Row>asList(new GenericRowWithSchema(new Object[]{from, count},
                    indirectMessageCountSchema)).iterator();
        }
    }
}
