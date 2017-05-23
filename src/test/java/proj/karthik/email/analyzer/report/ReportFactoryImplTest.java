package proj.karthik.email.analyzer.report;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.sql.Date;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import proj.karthik.email.analyzer.SparkTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link ReportFactory}
 */
public class ReportFactoryImplTest extends SparkTest {

    @Test
    public void testEmailCount() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.EMAIL_COUNT, simpleVolume1,
                explode(simpleVolume1));
        assertEquals(8, report.count());
    }

    @Test
    public void testDirectMessageCount() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.LARGEST_DIRECT_EMAILS,
                largeDirect, largeDirect);
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"nemec@mailman.enron.com", 11l}));
        verify(report, expectedRows);
    }

    @Test
    public void testIndirectMessageCount() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.LARGEST_BROADCAST_EMAILS,
                largeIndirect, largeIndirect);
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"drew.fossum@enron.com", 204l}),
                rowOf(new Object[]{"steven.kean@enron.com", 23l}),
                rowOf(new Object[]{"legalonline-compliance@enron.com", 8l})
        );
        verify(report, expectedRows);
    }

    @Test
    public void testFastResponseReport() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.FASTEST_RESPONSE_TIME,
                fastResponse, explode(fastResponse));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"<197504.1075840201540.JavaMail.evans@thyme>","Confidential",
                        60000l}),
                rowOf(new Object[]{"<28455164.1075842822881.JavaMail.evans@thyme>","Confidential " +
                        "Information and Securities Trading",120000l})
        );
        verify(report, expectedRows);
    }

    @Test
    public void testDailyEmailVolume() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.DAILY_EMAIL_VOLUME,
                simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{Date.valueOf("1999-10-18"), 24l}),
                rowOf(new Object[]{Date.valueOf("1999-11-18"), 42l}),
                rowOf(new Object[]{Date.valueOf("2001-03-20"), 9l})
        );
        verify(report, expectedRows);
    }

    @Test
    public void testDailyDirectSendVolume() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.DAILY_DIRECT_SEND_VOLUME,
                simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"nemec@enron.com", dateOf("2001-03-20"), 2l}),
                rowOf(new Object[]{"gerald.nemec@enron.com", dateOf("2001-03-20"), 7l}));
        verify(report, expectedRows);
    }

    @Test
    public void testDailyIndirectSendVolume() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.DAILY_INDIRECT_SEND_VOLUME,
                simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"steven.kean@enron.com", dateOf("1999-10-18"), 24l}),
                rowOf(new Object[]{"steven.kean@enron.com", dateOf("1999-11-18"), 42l}));
        verify(report, expectedRows);
    }

    @Test
    public void testHourlyDirectSendVolume() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.HOURLY_DIRECT_SEND_VOLUME,
                simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"gerald.nemec@enron.com", 0, 7l}),
                rowOf(new Object[]{"nemec@enron.com", 0, 2l}));
        verify(report, expectedRows);
    }

    @Test
    public void testHourlyIndirectSendVolume() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.HOURLY_INDIRECT_SEND_VOLUME,
                simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"steven.kean@enron.com", 0, 66l}));
        verify(report, expectedRows);
    }

    @Test
    public void testDailyDirectReceiveVolume() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType.DAILY_DIRECT_RECEIVE_VOLUME,
                simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"gerald.nemec@enron.com", dateOf("2001-03-20"), 2l}),
                rowOf(new Object[]{"nemec@mailman.enron.com", dateOf("2001-03-20"), 7l}));
        verify(report, expectedRows);
    }

    @Test
    public void testDailyIndirectReceiveVolume() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType
                         .DAILY_INDIRECT_RECEIVE_VOLUME, simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"kenneth.lay@enron.com", dateOf("1999-10-18"), 4l}),
                rowOf(new Object[]{"joseph.sutton@enron.com", dateOf("1999-10-18"), 4l}),
                rowOf(new Object[]{"mark.frevert@enron.com", dateOf("1999-10-18"), 4l}),
                rowOf(new Object[]{"joseph.sutton@enron.com", dateOf("1999-11-18"), 7l}),
                rowOf(new Object[]{"kenneth.lay@enron.com", dateOf("1999-11-18"), 7l}),
                rowOf(new Object[]{"mark.frevert@enron.com", dateOf("1999-11-18"), 7l}),
                rowOf(new Object[]{"mark.schroeder@enron.com", dateOf("1999-10-18"), 4l}),
                rowOf(new Object[]{"jeff.skilling@enron.com", dateOf("1999-10-18"), 4l}),
                rowOf(new Object[]{"john.sherriff@enron.com", dateOf("1999-10-18"), 4l}),
                rowOf(new Object[]{"jeff.skilling@enron.com", dateOf("1999-11-18"), 7l}),
                rowOf(new Object[]{"mark.schroeder@enron.com", dateOf("1999-11-18"), 7l}),
                rowOf(new Object[]{"john.sherriff@enron.com", dateOf("1999-11-18"), 7l}));
        verify(report, expectedRows);
    }

    @Test
    public void testHourlyDirectReceiveVolume() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType
                         .HOURLY_DIRECT_RECEIVE_VOLUME, simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"gerald.nemec@enron.com", 0, 2l}),
                rowOf(new Object[]{"nemec@mailman.enron.com", 0, 7l}));
        verify(report, expectedRows);
    }

    @Test
    public void testHourlyIndirectReceiveVolume() throws Exception {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType
                         .HOURLY_INDIRECT_RECEIVE_VOLUME, simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"mark.frevert@enron.com", 0, 11l}),
                rowOf(new Object[]{"mark.schroeder@enron.com", 0, 11l}),
                rowOf(new Object[]{"joseph.sutton@enron.com", 0, 11l}),
                rowOf(new Object[]{"john.sherriff@enron.com", 0, 11l}),
                rowOf(new Object[]{"kenneth.lay@enron.com", 0, 11l}),
                rowOf(new Object[]{"jeff.skilling@enron.com", 0, 11l})
        );
        verify(report, expectedRows);
    }

    @Test
    public void testBccPercentageReport() {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType
                .BCC_PERCENTAGE, bccTable, explode(bccTable));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{3l, 12l})
        );
        verify(report, expectedRows);
    }

    @Test
    public void testDomainReport() {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType
                .DOMAIN_PERCENTAGE, domainTable, explode(domainTable));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{10l, 2l})
        );
        verify(report, expectedRows);
    }

    @Test
    public void testTopLanguages() {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType
                .LANG_REPORT, languageTable, explode(languageTable));
        Queue<Row> actualRows = new LinkedList<>(report.collectAsList());
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"us-ascii", 5l}),
                rowOf(new Object[]{"iso-8859-1", 3l}),
                rowOf(new Object[]{"iso-8859-4", 2l}),
                rowOf(new Object[]{"iso-8859-3", 1l}),
                rowOf(new Object[]{"iso-8859-2", 1l}),
                rowOf(new Object[]{"iso-8859-5", 1l})
        );
        assertEquals(expectedRows.size(), actualRows.size());
        // We only compare first 3 as the rest of them can be in any order
        expectedRows.subList(0, 3).stream().forEach(row -> assertTrue(row.equals(actualRows
                 .remove())));
    }

    @Test
    public void testAttachment() {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType
                .ATTACHMENT_PERCENTAGE_REPORT, simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{20l, 20l})
        );
        verify(report, expectedRows);
    }

    @Test
    public void testTopSubjectReport() {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType
                .MESSAGE_THREAD_REPORT, simpleVolume1, explode(simpleVolume1));
        List<Row> expectedRows = Arrays.asList(
                rowOf(new Object[]{"Confidential Information and Securities Trading", 7l}),
                rowOf(new Object[]{"Confidential",6l}),
                rowOf(new Object[]{"Translation of articles", 3l})
        );
        verify(report, expectedRows);
    }

    @Test
    public void testTopConnections() {
        Dataset<Row> report = reportFactory.generate(PrebuildReportType
                .CONNECTIONS_REPORT, connectionsTable, explode(connectionsTable));
        assertEquals(8, report.collectAsList().size());
    }

    private void verify(final Dataset<Row> report, final List<Row> expectedRows) {
        Queue<Row> actualRows = new LinkedList<>(report.collectAsList());
        assertEquals(expectedRows.size(), actualRows.size());
        expectedRows.stream().forEach(row -> assertTrue(row.equals(actualRows.remove())));
    }
}
