package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.model.Header;

/**
 * Parse the email timestamp from the email document.
 * Assumption: In order to be flexible, multiple different timestamp formats like are
 * RFC 1123, ISO_DATE_TIME, ISO_LOCAL_DATE_TIME, ISO_INSTANT, ISO_OFFSET_DATE_TIME, etc are
 * supported.
 * Expected line format: "Date: Wed, 28 Feb 2001 06:49:00 -0800 (PST)"
 */
public class DateParser extends BaseTwoPartParser<Header> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DateParser.class);
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("(^Date: )(.*)",
            Pattern.CASE_INSENSITIVE);
    private static final DateTimeFormatter[] SUPPORTED_FORMATS = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("E, d MMM yyyy H:m:s Z (z)"),
            DateTimeFormatter.ISO_DATE_TIME, DateTimeFormatter.ISO_INSTANT,
            DateTimeFormatter.ISO_LOCAL_DATE_TIME, DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.RFC_1123_DATE_TIME
    };

    @Inject
    public DateParser() {
        super("timestamp", TIMESTAMP_PATTERN);
    }

    @Override
    protected void process(final String timestamp, final Header header) {
        LOGGER.debug("Applying timestamp parsing rule to {}", timestamp);
        header.setTimestamp(parseTimestamp(timestamp));
    }

    private Timestamp parseTimestamp(final String timestamp) {
        final Timestamp[] timestamps = {null};
        Arrays.stream(SUPPORTED_FORMATS).anyMatch(dateTimeFormatter -> {
            LOGGER.debug("Tring to apply {} format to {}", dateTimeFormatter.toString(),
                    timestamp);
            try {
                if (dateTimeFormatter.equals(DateTimeFormatter.ISO_LOCAL_DATE_TIME)) {
                    LOGGER.debug("Parsing as local datetime");
                    timestamps[0] = Timestamp.valueOf(LocalDateTime.parse(timestamp,
                            dateTimeFormatter));
                } else {
                    LOGGER.debug("Parsing as zoned datetime");
                    timestamps[0] = Timestamp.from(ZonedDateTime.parse(timestamp,
                            dateTimeFormatter).toInstant());
                }
                return true;
            } catch (DateTimeParseException e) {
                LOGGER.debug("Skipping {} datetime formatter as the pattern did not match",
                        dateTimeFormatter.toString());
                return false;
            }
        });
        if (timestamps[0] != null) {
            return timestamps[0];
        }
        throw new AnalyzerException("Unable to match timestamp pattern for input:%s", timestamp);
    }
}
