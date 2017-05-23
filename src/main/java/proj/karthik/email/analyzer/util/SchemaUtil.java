package proj.karthik.email.analyzer.util;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that manages the intermediate schemas.
 */
public class SchemaUtil {

    private static final StructType receiveCountSchema;
    private static StructType directMessageCountSchema;
    private static StructType indirectMessageCountSchema;
    private static StructType timeDiffSchema;
    private static StructType explodedTableSchema;
    private static StructType bccReportSchema;
    private static StructType domainReportSchema;
    private static StructType attachmentReportSchema;

    static {
        List<StructField> receiveCountFields = new ArrayList<>(2);
        receiveCountFields.add(new StructField("date", DataTypes.DateType, false,
                Metadata.empty()));
        receiveCountFields.add(new StructField("receiveCount", DataTypes.LongType, false,
                Metadata.empty()));
        receiveCountSchema = new StructType(receiveCountFields.toArray(new StructField[0]));

        List<StructField> bccReportFields = new ArrayList<>(2);
        bccReportFields.add(new StructField("bcc_count", DataTypes.LongType, false,
                Metadata.empty()));
        bccReportFields.add(new StructField("total_count", DataTypes.LongType, false,
                Metadata.empty()));
        bccReportSchema = new StructType(bccReportFields.toArray(new StructField[0]));

        List<StructField> attachmentReportFields = new ArrayList<>(2);
        attachmentReportFields.add(new StructField("attachment_count", DataTypes.LongType, false,
                Metadata.empty()));
        attachmentReportFields.add(new StructField("total_count", DataTypes.LongType, false,
                Metadata.empty()));
        attachmentReportSchema = new StructType(attachmentReportFields.toArray(new StructField[0]));

        List<StructField> domainReportFields = new ArrayList<>(2);
        domainReportFields.add(new StructField("same_domain", DataTypes.LongType, false,
                Metadata.empty()));
        domainReportFields.add(new StructField("different_domain", DataTypes.LongType, false,
                Metadata.empty()));
        domainReportSchema = new StructType(domainReportFields.toArray(new StructField[0]));

        List<StructField> directMessageCountFields = new ArrayList<>(2);
        directMessageCountFields.add(new StructField("emailId", DataTypes.StringType, false,
                Metadata.empty()));
        directMessageCountFields.add(new StructField("directCount", DataTypes.LongType, false,
                Metadata.empty()));
        directMessageCountSchema = new StructType(directMessageCountFields.toArray(
                new StructField[0]));

        List<StructField> indirectMessageCountFields = new ArrayList<>(2);
        indirectMessageCountFields.add(new StructField("emailId", DataTypes.StringType, false,
                Metadata.empty()));
        indirectMessageCountFields.add(new StructField("indirectCount", DataTypes.LongType, false,
                Metadata.empty()));
        indirectMessageCountSchema = new StructType(indirectMessageCountFields.toArray(
                new StructField[0]));

        List<StructField> explodedFields = new ArrayList<>(10);
        explodedFields.add(new StructField("id", DataTypes.StringType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("charset", DataTypes.StringType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("content_type", DataTypes.StringType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("tstamp", DataTypes.TimestampType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("subject", DataTypes.StringType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("sender", DataTypes.StringType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("sender_domain", DataTypes.StringType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("receiver", DataTypes.StringType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("receiver_domain", DataTypes.StringType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("receiver_type", DataTypes.StringType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("is_direct", DataTypes.BooleanType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("is_reply", DataTypes.BooleanType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("is_forward", DataTypes.BooleanType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("has_attachment", DataTypes.BooleanType, false,
                Metadata.empty()));
        explodedFields.add(new StructField("origin_tstamp", DataTypes.TimestampType, true,
                Metadata.empty()));
        explodedTableSchema = new StructType(explodedFields.toArray(new StructField[0]));

        List<StructField> timeDiffFields = new ArrayList<>(2);
        timeDiffFields.add(new StructField("id", DataTypes.StringType, false,
                Metadata.empty()));
        timeDiffFields.add(new StructField("subject_text", DataTypes.StringType, false,
                Metadata.empty()));
        timeDiffFields.add(new StructField("response_time_in_millis", DataTypes.LongType, false,
                Metadata.empty()));
        timeDiffSchema = new StructType(timeDiffFields.toArray(new StructField[0]));
    }

    public SchemaUtil() {
    }

    public static StructType getReceiveCountSchema() {
        return receiveCountSchema;
    }

    public static StructType getIndirectMessageCountSchema() {
        return indirectMessageCountSchema;
    }

    public static StructType getDirectMessageCountSchema() {
        return directMessageCountSchema;
    }

    public static StructType getTimeDiffSchema() {
        return timeDiffSchema;
    }

    public static StructType getExplodedTableSchema() {
        return explodedTableSchema;
    }

    public static StructType getBccReportSchema() {
        return bccReportSchema;
    }

    public static StructType getDomainReportSchema() {
        return domainReportSchema;
    }

    public static StructType getAttachmentReportSchema() {
        return attachmentReportSchema;
    }
}
