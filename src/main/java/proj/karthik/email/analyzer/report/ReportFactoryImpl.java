package proj.karthik.email.analyzer.report;

import com.google.inject.Inject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.util.ReportUtil;

/**
 * Implementation of {@link ReportFactory}
 */
public class ReportFactoryImpl implements ReportFactory  {

    private final ReportUtil reportUtil;

    @Inject
    public ReportFactoryImpl(ReportUtil reportUtil) {
        this.reportUtil = reportUtil;
    }

    @Override
    public Dataset<Row> generate(PrebuildReportType prebuildReportType, Dataset<Row> dataset,
            Dataset<Row> exploded) {
        switch (prebuildReportType) {
            case EMAIL_COUNT:
                return ((Report) reportUtil::generateDailyEmailCount)
                        .runReport(dataset, exploded);
            case LARGEST_DIRECT_EMAILS:
                return ((Report) reportUtil::generateTopDirectMessageReport)
                        .runReport(dataset, exploded);
            case LARGEST_BROADCAST_EMAILS:
                return ((Report) reportUtil::generateTopIndirectMessageReport)
                        .runReport(dataset, exploded);
            case FASTEST_RESPONSE_TIME:
                return ((Report) reportUtil::generateFastestResponseTimeReport)
                        .runReport(dataset, exploded);
            case DAILY_EMAIL_VOLUME:
                return ((Report) reportUtil::generateDailyEmailVolumeReport)
                        .runReport(dataset, exploded);
            case HOURLY_EMAIL_VOLUME:
                return ((Report) reportUtil::generateHourlyEmailVolumeReport)
                        .runReport(dataset, exploded);
            case DAILY_DIRECT_SEND_VOLUME:
                return ((Report) reportUtil::generateDailyDirectSendVolumeReport)
                        .runReport(dataset, exploded);
            case DAILY_INDIRECT_SEND_VOLUME:
                return ((Report) reportUtil::generateDailyIndirectSendVolumeReport)
                        .runReport(dataset, exploded);
            case HOURLY_DIRECT_SEND_VOLUME:
                return ((Report) reportUtil::generateHourlyDirectSendVolumeReport)
                        .runReport(dataset, exploded);
            case HOURLY_INDIRECT_SEND_VOLUME:
                return ((Report) reportUtil::generateHourlyIndirectSendVolumeReport)
                        .runReport(dataset, exploded);
            case DAILY_DIRECT_RECEIVE_VOLUME:
                return ((Report) reportUtil::generateDailyDirectReceiveVolumeReport)
                        .runReport(dataset, exploded);
            case DAILY_INDIRECT_RECEIVE_VOLUME:
                return ((Report) reportUtil::generateDailyIndirectReceiveVolumeReport)
                        .runReport(dataset, exploded);
            case HOURLY_DIRECT_RECEIVE_VOLUME:
                return ((Report) reportUtil::generateHourlyDirectReceiveVolumeReport)
                        .runReport(dataset, exploded);
            case HOURLY_INDIRECT_RECEIVE_VOLUME:
                return ((Report) reportUtil::generateHourlyIndirectReceiveVolumeReport)
                        .runReport(dataset, exploded);
            case BCC_PERCENTAGE:
                return ((Report) reportUtil::generateBccReport)
                        .runReport(dataset, exploded);
            case DOMAIN_PERCENTAGE:
                return ((Report) reportUtil::generateDomainReport)
                        .runReport(dataset, exploded);
            case LANG_REPORT:
                return ((Report) reportUtil::generateLanguageReport)
                        .runReport(dataset, exploded);
            case ATTACHMENT_PERCENTAGE_REPORT:
                return ((Report) reportUtil::generateAttachmentReport)
                        .runReport(dataset, exploded);
            case MESSAGE_THREAD_REPORT:
                return ((Report) reportUtil::generateMessageThreadReport)
                        .runReport(dataset, exploded);
            case CONNECTIONS_REPORT:
                return ((Report) reportUtil::generateConnectionsReport)
                        .runReport(dataset, exploded);

        }
        throw new AnalyzerException("Unknown report code: %s", prebuildReportType.toString());
    }

}
