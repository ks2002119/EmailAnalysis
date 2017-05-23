package proj.karthik.email.analyzer.util;

import com.google.inject.Inject;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import proj.karthik.email.analyzer.report.spark.functions.DirectMessageCountFunction;
import proj.karthik.email.analyzer.report.spark.functions.IndirectMessageCountFunction;
import proj.karthik.email.analyzer.report.spark.functions.ReceiveCountFunction;
import proj.karthik.email.analyzer.report.spark.functions.TimediffFunction;

import static org.apache.spark.sql.functions.hour;

/**
 * Utility class for generating reports.
 */
public class ReportUtil {
    private final SQLContext sqlContext;

    @Inject
    public ReportUtil(final SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    /**
     * Generates a report to show the percentage of communication that happens using 'bcc' model.
     *
     * @param table
     * @param exploded
     * @return
     */
    public Dataset<Row> generateBccReport(Dataset<Row> table, Dataset<Row> exploded) {
        Dataset<Row> bccDataset = exploded.filter(exploded.col("receiver_type").equalTo("bcc"));
        Long bccMessages = bccDataset.count();
        Long totalMessages = exploded.count();
        StructType bccReportSchema = SchemaUtil.getBccReportSchema();
        Row row = new GenericRowWithSchema(new Object[] {bccMessages, totalMessages},
                bccReportSchema);
        return sqlContext.createDataFrame(Arrays.asList(row), bccReportSchema);
    }

    /**
     * Generates the statistics about number of emails with attachments.
     *
     * @param dataset
     * @param exploded
     * @return attachmentReport
     */
    public Dataset<Row> generateAttachmentReport(Dataset<Row> dataset, Dataset<Row> exploded) {
        Dataset<Row> attachmentDataset = dataset.filter(dataset.col("header.attachment")
                 .isNotNull());
        Long totalMessages = dataset.count();
        Long totalAttachments = attachmentDataset.count();
        StructType attachmentReportSchema = SchemaUtil.getAttachmentReportSchema();
        Row row = new GenericRowWithSchema(new Object[] {totalAttachments, totalMessages},
                attachmentReportSchema);
        return sqlContext.createDataFrame(Arrays.asList(row), attachmentReportSchema);
    }

    /**
     * Generates a count showing the amount of emails that went from one domain to another.
     *
     * @param dataset
     * @param exploded
     * @return dataset
     */
    public Dataset<Row> generateDomainReport(Dataset<Row> dataset, Dataset<Row> exploded) {
        Dataset<Row> differentDomain = exploded.filter(
                exploded.col("receiver_domain").notEqual(exploded.col("sender_domain")));
        Dataset<Row> sameDomain = exploded.filter(
                exploded.col("receiver_domain").equalTo(exploded.col("sender_domain")));
        StructType domainReportSchema = SchemaUtil.getDomainReportSchema();
        Long sameDomainCount = sameDomain.count();
        Long differentDomainCount = differentDomain.count();
        Row row = new GenericRowWithSchema(new Object[] {sameDomainCount,
                differentDomainCount}, domainReportSchema);
        return sqlContext.createDataFrame(Arrays.asList(row), domainReportSchema);
    }

    /**
     * Generates a report containing top 10 language/charsets used.
     *
     * @param dataset
     * @param exploded
     * @return report
     */
    public Dataset<Row> generateLanguageReport(Dataset<Row> dataset, Dataset<Row> exploded) {
        Dataset<Row> count = exploded.dropDuplicates("id", "charset")
                .groupBy(exploded.col("charset"))
                .count();
        return count.sort(count.col("count").desc()).limit(10);
    }

    /**
     * Generates a report that shows the emails with most number of exchanges.
     *
     * @param baseDataset
     * @param exploded
     * @return dataset
     */
    public Dataset<Row> generateMessageThreadReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        Dataset<Row> expanded = baseDataset.select(
                baseDataset.col("header.subject.text").as("subject_text"),
                baseDataset.col("header.subject.is_reply").as("is_reply"));
        expanded = expanded.filter(expanded.col("is_reply").isNotNull()
                .and(expanded.col("is_reply").equalTo(Boolean.TRUE)));
        Dataset<Row> countDataset = expanded.groupBy(expanded.col("subject_text")).count();
        return countDataset.sort(countDataset.col("count").desc()).limit(10);
    }

    /**
     * Generates a report on "How many emails did each person receive each day?"
     * 1. Generate one row per email present in "to" email fields - "id", "email_id", "date"
     * 2. Generate one row per email present in "cc" email fields - "id", "email_id", "date"
     * 3. Generate one row per email present in "bcc" email fields - "id", "email_id", "date"
     * 4. Combine these datasets and remove duplicate rows with same values
     * in the columns - "id", "email_id", "date"
     * 5. Groupby "email_id" and "date" and compute "sum"
     *
     * @param table
     * @return result
     */
    public Dataset<Row> generateDailyEmailCount(Dataset<Row> table, Dataset<Row> exploded) {
        exploded = exploded.withColumn("date", exploded.col("tstamp").cast("date"));
        exploded = exploded.dropDuplicates("receiver", "id");
        return exploded.groupBy(exploded.col("date"), exploded.col("receiver"))
                .count();
    }

    /**
     * Generates a report on "Person (or people) who received the largest number of direct emails"
     * 1. Iterate over all the emails and get to, cc and bcc fields.
     * 2. For each email, if there is only one "to" and no 'cc' or 'bcc', it is counted as a
     * direct message to that user.
     * 3. Finally, Group by "email" and compute sum.
     *
     * @param baseDataset
     * @return result
     */
    public Dataset<Row> generateTopDirectMessageReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        Dataset<Row> expanded = baseDataset.select(
                baseDataset.col("header.to.id").as("to_email"),
                baseDataset.col("header.cc.id").as("cc_email"),
                baseDataset.col("header.bcc.id").as("bcc_email"));
        JavaRDD<Row> directCountRDD = expanded.javaRDD()
                .flatMap(new DirectMessageCountFunction());
        Dataset<Row> directCount = sqlContext.createDataFrame(directCountRDD,
                SchemaUtil.getDirectMessageCountSchema());
        Dataset<Row> aggregate = directCount.groupBy(directCount.col("emailId"))
                .sum("directCount");
        return aggregate.sort(aggregate.col("sum(directCount)").desc());
    }

    /**
     * Generates a report on "Person (or people) who sent the largest number of broadcast emails"
     * 1. Iterate over all the emails and get from, to, cc and bcc fields.
     * 2. For each email, if there is only one "to" and no 'cc' or 'bcc', it is counted as a
     * direct message to that user.
     * 3. Finally, Group by "email" and compute sum.
     *
     * @param baseDataset
     * @return result
     */
    public Dataset<Row> generateTopIndirectMessageReport(Dataset<Row> baseDataset,
            Dataset<Row> expanded) {
        Dataset<Row> exploded = baseDataset.select(
                baseDataset.col("header.from.id").as("from"),
                baseDataset.col("header.to.id").as("to_email"),
                baseDataset.col("header.cc.id").as("cc_email"),
                baseDataset.col("header.bcc.id").as("bcc_email"));
        JavaRDD<Row> directCountRDD = exploded.javaRDD()
                .flatMap(new IndirectMessageCountFunction());
        Dataset<Row> directCount = sqlContext.createDataFrame(directCountRDD,
                SchemaUtil.getIndirectMessageCountSchema());
        Dataset<Row> aggregate = directCount.groupBy(directCount.col("emailId"))
                .sum("indirectCount");
        return aggregate.sort(aggregate.col("sum(indirectCount)").desc());
    }

    /**
     * Computes the difference between origin timestamp and received timestamp and ranks them to
     * show the emails with fastest reponse time.
     *
     * @param baseDataset
     * @param exploded
     * @return report
     */
    public Dataset<Row> generateFastestResponseTimeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        baseDataset = baseDataset.withColumnRenamed("timestamp", "tstamp")
                .withColumn("origin_tstamp", baseDataset.col("body.sent_at")
                        .cast("timestamp"))
                .withColumn("id", baseDataset.col("header.id"))
                .withColumn("is_reply", baseDataset.col("header.subject.is_reply"))
                .withColumn("subject", baseDataset.col("header.subject.text"));
        baseDataset.show(10);
        baseDataset = baseDataset.select(baseDataset.col("id"), baseDataset.col("is_reply"),
                        baseDataset.col("subject"), baseDataset.col("tstamp"), baseDataset.col
                                ("origin_tstamp"));
        baseDataset.show(10);
        baseDataset = baseDataset.filter(baseDataset.col("is_reply").isNotNull()
                .and(baseDataset.col("is_reply").equalTo(Boolean.TRUE))
                .and(baseDataset.col("origin_tstamp").isNotNull()));
        baseDataset.show(10);
        JavaRDD<Row> timediffRDD = baseDataset.javaRDD().map(new TimediffFunction());
        Dataset<Row> timeDiffDataset = sqlContext.createDataFrame(timediffRDD,
                SchemaUtil.getTimeDiffSchema());
        baseDataset.show(10);
        return timeDiffDataset.sort(timeDiffDataset.col("response_time_in_millis").asc()).limit(5);
    }

    /**
     * Generates the connections report.
     *
     * @param dataset
     * @param exploded
     * @return report
     */
    public Dataset<Row> generateConnectionsReport(Dataset<Row> dataset, Dataset<Row> exploded) {
        Dataset<Row> connections = exploded.select(exploded.col("sender"),
                exploded.col("receiver"), exploded.col("id"))
                .dropDuplicates("sender", "receiver", "id");
        Dataset<Row> count = connections.groupBy(connections.col("sender"),
                connections.col("receiver")).count();
        return count.sort(count.col("count").desc()).limit(10);
    }

    /**
     * Generates daily email volume report.
     *
     * @param baseDataset
     * @param exploded
     * @return report
     */
    public Dataset<Row> generateDailyEmailVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        // Convert timestamp to date for our aggregation.
        baseDataset = baseDataset.withColumn("date", baseDataset.col("header.timestamp")
                .cast("date"));
        Dataset<Row> expanded = baseDataset.select(baseDataset.col("date"),
                baseDataset.col("header.to.id").as("to_email"),
                baseDataset.col("header.cc.id").as("cc_email"),
                baseDataset.col("header.bcc.id").as("bcc_email"));
        JavaRDD<Row> receiveCountRDD = expanded.javaRDD()
                .map(new ReceiveCountFunction());
        Dataset<Row> receive = sqlContext.createDataFrame(receiveCountRDD,
                SchemaUtil.getReceiveCountSchema());
        return receive.groupBy(receive.col("date")).sum("receiveCount");
    }

    public Dataset<Row> generateHourlyEmailVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        // Convert timestamp to date for our aggregation.
        baseDataset = baseDataset.withColumn("date", baseDataset.col("header.timestamp")
                .cast("date"));
        Dataset<Row> expanded = baseDataset.select(baseDataset.col("date"),
                baseDataset.col("header.to.id").as("to_email"),
                baseDataset.col("header.cc.id").as("cc_email"),
                baseDataset.col("header.bcc.id").as("bcc_email"));
        JavaRDD<Row> receiveCountRDD = expanded.javaRDD()
                .map(new ReceiveCountFunction());
        Dataset<Row> receive = sqlContext.createDataFrame(receiveCountRDD,
                SchemaUtil.getReceiveCountSchema());
        return receive.groupBy(hour(receive.col("date")).as("hour")).sum("receiveCount");
    }

    public Dataset<Row> generateDailyDirectSendVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        return generateDailyVolumeReport(exploded, true, "sender");
    }

    public Dataset<Row> generateDailyIndirectSendVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        return generateDailyVolumeReport(exploded, false, "sender");
    }

    public Dataset<Row> generateHourlyDirectSendVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        return generateHourlyVolumeReport(exploded, true, "sender");
    }

    public Dataset<Row> generateHourlyIndirectSendVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        return generateHourlyVolumeReport(exploded, false, "sender");
    }

    public Dataset<Row> generateDailyDirectReceiveVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        return generateDailyVolumeReport(exploded, true, "receiver");
    }

    public Dataset<Row> generateDailyIndirectReceiveVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        return generateDailyVolumeReport(exploded, false, "receiver");
    }

    public Dataset<Row> generateHourlyDirectReceiveVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        return generateHourlyVolumeReport(exploded, true, "receiver");
    }

    public Dataset<Row> generateHourlyIndirectReceiveVolumeReport(Dataset<Row> baseDataset,
            Dataset<Row> exploded) {
        return generateHourlyVolumeReport(exploded, false, "receiver");
    }

    private Dataset<Row> generateDailyVolumeReport(Dataset<Row> exploded, boolean isDirect,
            String aggField) {
        exploded = exploded.withColumn("date", exploded.col("tstamp").cast("date"));
        if (isDirect) {
            exploded = exploded.filter(
                    exploded.col("is_direct").equalTo(Boolean.TRUE));
        } else {
            exploded = exploded.filter(
                    exploded.col("is_direct").equalTo(Boolean.FALSE));
        }
        return exploded.groupBy(exploded.col(aggField), exploded.col("date"))
                .count();
    }

    private Dataset<Row> generateHourlyVolumeReport(Dataset<Row> exploded, boolean isDirect,
            String aggField) {
        exploded = exploded.withColumn("date", exploded.col("tstamp").cast("date"));
        if (isDirect) {
            exploded = exploded.filter(
                    exploded.col("is_direct").equalTo(Boolean.TRUE));
        } else {
            exploded = exploded.filter(
                    exploded.col("is_direct").equalTo(Boolean.FALSE));
        }
        return exploded.groupBy(exploded.col(aggField), hour(exploded.col("date")).as("hour"))
                .count();
    }
}
