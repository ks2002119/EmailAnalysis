package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import java.util.regex.Pattern;

import proj.karthik.email.analyzer.model.Header;

/**
 * Parse the message id as string from the email document
 * Expected line format: "Message-ID: <29023172.1075847627587.JavaMail.evans@thyme>"
 */
public class MessageIdParser extends BaseTwoPartParser<Header> {
    private static final Pattern MESSAGE_ID_PATTERN = Pattern.compile("(^Message-ID: )(.*)",
            Pattern.CASE_INSENSITIVE);

    @Inject
    public MessageIdParser() {
        super("id", MESSAGE_ID_PATTERN);
    }

    @Override
    public void process(final String input, final Header header) {
        header.setId(input);
    }
}
