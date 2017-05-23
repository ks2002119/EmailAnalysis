package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link DateParser}
 */
public class DateParserTest {
    private static final DateParser TIMESTAMP_PARSER = new DateParser();

    @Test
    public void testCustomTimestampParsing() throws Exception {
        Header header = new Header();
        TIMESTAMP_PARSER.parse("Date: Thu, 19 Apr 2001 03:45:00 -0700 (PDT)", header);
        assertEquals(ZonedDateTime.parse("Thu, 19 Apr 2001 03:45:00 -0700 (PDT)",
                DateTimeFormatter.ofPattern("E, dd MMM yyyy HH:mm:ss Z (z)")).toInstant()
                        .toEpochMilli(), header.timestamp().toInstant().toEpochMilli());
    }

    @Test
    public void testISOTimestampParsing() throws Exception {
        Header header = new Header();
        TIMESTAMP_PARSER.parse("Date: 2011-12-03T10:15:30", header);
        assertEquals(Timestamp.valueOf(LocalDateTime.parse("2011-12-03T10:15:30")),
                header.timestamp());
        header = new Header();
        TIMESTAMP_PARSER.parse("Date: 2011-12-03T10:15:30+01:00", header);
        assertEquals(ZonedDateTime.parse("2011-12-03T10:15:30+01:00",
                DateTimeFormatter.ISO_DATE_TIME).toInstant().toEpochMilli(),
                header.timestamp().toInstant().toEpochMilli());
        TIMESTAMP_PARSER.parse("Date: 2011-12-03T10:15:30+01:00[Europe/Paris]", header);
        assertEquals(ZonedDateTime.parse("2011-12-03T10:15:30+01:00[Europe/Paris]",
                DateTimeFormatter.ISO_DATE_TIME).toInstant().toEpochMilli(),
                header.timestamp().toInstant().toEpochMilli());
    }

    @Test
    public void testISOInstantTimestampParsing() throws Exception {
        Header header = new Header();
        TIMESTAMP_PARSER.parse("Date: 2011-12-03T10:15:30Z", header);
        assertEquals(ZonedDateTime.parse("2011-12-03T10:15:30Z").toInstant().toEpochMilli(),
                header.timestamp().toInstant().toEpochMilli());
    }

    @Test
    public void testISOLocalTimestampParsing() throws Exception {
        Header header = new Header();
        TIMESTAMP_PARSER.parse("Date: 2011-12-03T10:15:30", header);
        assertEquals(Timestamp.valueOf(LocalDateTime.parse("2011-12-03T10:15:30")),
                header.timestamp());
    }

    @Test
    public void testISOTimestampWithOffsetParsing() throws Exception {
        Header header = new Header();
        TIMESTAMP_PARSER.parse("Date: 2011-12-03T10:15:30+01:00", header);
        assertEquals(ZonedDateTime.parse("2011-12-03T10:15:30+01:00").toInstant().toEpochMilli(),
                header.timestamp().toInstant().toEpochMilli());
    }

    @Test
    public void testRFC1123TimestampParsing() throws Exception {
        Header header = new Header();
        TIMESTAMP_PARSER.parse("Date: Tue, 3 Jun 2008 11:05:30 GMT", header);
        assertEquals(ZonedDateTime.parse("Tue, 3 Jun 2008 11:05:30 GMT",
                DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli(),
                header.timestamp().toInstant().toEpochMilli());
    }

    @Test
    public void testTimestampParsingError() throws Exception {
        Header header = new Header();
        try {
            TIMESTAMP_PARSER.parse("Date: Thu, 19 Apr 2001", header);
        } catch (Exception e) {
            assertEquals("Unable to match timestamp pattern for input:Thu, 19 Apr 2001", e
                     .getMessage());
        }
    }
}
