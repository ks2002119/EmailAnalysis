package proj.karthik.email.analyzer.report.spark.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import proj.karthik.email.analyzer.util.SchemaUtil;

import static scala.collection.JavaConversions.seqAsJavaList;

/**
 * Reformats the table as a exploded table.
 */
public class FieldExploder implements FlatMapFunction<Row, Row>, Serializable {

    private transient StructType explodedTableSchema;

    public FieldExploder() {
        explodedTableSchema = SchemaUtil.getExplodedTableSchema();
    }


    @Override
    public Iterator<Row> call(final Row row) throws Exception {
        List<Object[]> rows = getObjects(row);
        return rows.stream().map(objects -> new GenericRowWithSchema(objects, explodedTableSchema))
                .collect(Collectors.<Row>toList()).iterator();
    }

    private List<Object[]> getObjects(final Row row) {
        Timestamp receivedTstamp = row.getTimestamp(row.fieldIndex("received_tstamp"));
        Timestamp originTstamp = row.getTimestamp(row.fieldIndex("sent_tstamp"));
        Row header = (Row) row.get(row.fieldIndex("header"));
        Row fromRow = (Row) header.get(header.fieldIndex("from"));
        String from = fromRow.getString(fromRow.fieldIndex("id"));
        String fromDomain = fromRow.getString(fromRow.fieldIndex("domain"));
        Row subject = (Row) header.get(header.fieldIndex("subject"));
        int charsetIndex = header.fieldIndex("charset");
        int contentTypeIndex = header.fieldIndex("content_type");
        int isReplyIndex = subject.fieldIndex("is_reply");
        int isForwardIndex = subject.fieldIndex("is_forward");
        int attachmentIndex = header.fieldIndex("attachment");
        int idIndex = header.fieldIndex("id");
        int subjectTextIndex = subject.fieldIndex("text");

        Map<String, String> cc = reformat(seqAsJavaList(header.getSeq(header.fieldIndex("cc"))));
        Map<String, String> bcc = reformat(seqAsJavaList(header.getSeq(header.fieldIndex("bcc"))));
        Map<String, String> to = reformat(seqAsJavaList(header.getSeq(header.fieldIndex("to"))));
        boolean isDirect = to.size() == 1 && cc.isEmpty() && bcc.isEmpty();

        String msgId = header.getString(idIndex);
        String charset = header.getString(charsetIndex);
        String contentType = header.getString(contentTypeIndex);
        String subjectText = subject.getString(subjectTextIndex);
        boolean isReply = subject.isNullAt(isReplyIndex)? false : subject.getBoolean(isReplyIndex);
        boolean isForward = subject.isNullAt(isForwardIndex)? false :
                subject.getBoolean(isForwardIndex);
        boolean hasAttachment = !header.isNullAt(attachmentIndex);
        List<Object[]> values = cc.entrySet().stream().map(ccEmail -> new Object[]{
                msgId, charset, contentType, receivedTstamp, subjectText,
                from, fromDomain, ccEmail.getKey(), ccEmail.getValue(), "cc", isDirect,
                isReply, isForward, hasAttachment, originTstamp
        }).collect(Collectors.toList());
        values.addAll(bcc.entrySet().stream().map(bccEmail -> new Object[]{
                msgId, charset, contentType, receivedTstamp, subjectText,
                from, fromDomain, bccEmail.getKey(), bccEmail.getValue(), "bcc", isDirect,
                isReply, isForward, hasAttachment, originTstamp
        }).collect(Collectors.toList()));
        values.addAll(to.entrySet().stream().map(toEmail -> new Object[]{
                msgId, charset, contentType, receivedTstamp, subjectText,
                from, fromDomain, toEmail.getKey(), toEmail.getValue(), "to", isDirect,
                isReply, isForward, hasAttachment, originTstamp
        }).collect(Collectors.toList()));
        return values;
    }

    private Map<String, String> reformat(List<Row> rows) {
        Map<String, String> keyValues = new LinkedHashMap<>();
        rows.stream().forEach(row -> {
            String id = row.getString(row.fieldIndex("id"));
            if (row.isNullAt(row.fieldIndex("domain"))) {
                keyValues.put(id, "");
            } else {
                keyValues.put(id, row.getString(row.fieldIndex("domain")));
            }
        });
        return keyValues;
    }
}
