package proj.karthik.email.analyzer.report;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum for prebuilt reports
 */
public enum PrebuildReportType {
    EMAIL_COUNT(1, "How many emails did each person receive each day?"),
    LARGEST_DIRECT_EMAILS(2, "Person (or people) who received the largest number of direct " +
            "emails"),
    LARGEST_BROADCAST_EMAILS(3, "Person (or people) who sent the largest number of broadcast " +
            "emails"),
    FASTEST_RESPONSE_TIME(4, "Find the five emails with the fastest response times"),
    DAILY_EMAIL_VOLUME(5, "Find number of emails received by everyone on daily basis"),
    HOURLY_EMAIL_VOLUME(6, "Find number of emails received by everyone on hourly basis"),
    DAILY_DIRECT_SEND_VOLUME(7, "Volume of direct emails sent by everyone on daily basis"),
    DAILY_INDIRECT_SEND_VOLUME(8, "Volume of indirect emails sent by everyone on daily basis"),
    HOURLY_DIRECT_SEND_VOLUME(9, "Volume of direct emails sent by everyone on hourly basis"),
    HOURLY_INDIRECT_SEND_VOLUME(10, "Volume of indirect emails sent by everyone on hourly basis"),
    DAILY_DIRECT_RECEIVE_VOLUME(11, "Volume of direct emails received by everyone on daily basis"),
    DAILY_INDIRECT_RECEIVE_VOLUME(12, "Volume of indirect emails received by everyone on daily " +
            "basis"),
    HOURLY_DIRECT_RECEIVE_VOLUME(13, "Volume of direct emails received by everyone on hourly " +
            "basis"),
    HOURLY_INDIRECT_RECEIVE_VOLUME(14, "Volume of indirect emails received by everyone on hourly " +
            "basis"),
    BCC_PERCENTAGE(15, "Percentage of communication that happens using 'bcc'"),
    DOMAIN_PERCENTAGE(16, "Percentage of communication that happens across different " +
            "organizations"),
    LANG_REPORT(17, "Top language/charsets used"),
    ATTACHMENT_PERCENTAGE_REPORT(18, "Percentage of emails that had attachment"),
    MESSAGE_THREAD_REPORT(19, "Message threads with largest number of emails exchanged"),
    CONNECTIONS_REPORT(20, "Top connections");

    private static final Map<Integer, PrebuildReportType> REPORTS_MAP = new HashMap<>();

    static {
        Arrays.stream(PrebuildReportType.values())
                .forEach(prebuildReports -> {
                    REPORTS_MAP.put(prebuildReports.getCode(), prebuildReports);
                });
    }

    public static PrebuildReportType getReportEnumBy(int code) {
        return REPORTS_MAP.get(code);
    }

    private final int code;
    private final String message;
    PrebuildReportType(final int reportCode, final String message) {
        this.code = reportCode;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
