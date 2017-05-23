package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.model.Body;

/**
 * Created by ksubramanian on 3/2/17.
 */
public class SentParser extends BaseTwoPartParser<Body> {
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("(^Sent: )(.*)",
            Pattern.CASE_INSENSITIVE);
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
            .ofPattern("E, L d, yyyy h:m a");
    @Inject
    public SentParser() {
        super("sent", TIMESTAMP_PATTERN);
    }

    @Override
    protected void process(final String timestamp, final Body body) {
        body.setEmbeddedTimestamp(Timestamp.from(LocalDateTime.parse(timestamp,
                DATE_TIME_FORMATTER).toInstant(ZoneOffset.of(TimeZone.getDefault().getID())))) ;
    }
}
