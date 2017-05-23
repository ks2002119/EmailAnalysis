package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import java.util.regex.Pattern;

import proj.karthik.email.analyzer.model.Header;

/**
 * Parse the "from" address from the email document
 * Expected line format: "From: steven.kean@enron.com"
 */
public class FromAddressParser extends BaseTwoPartParser<Header> {
    private static final Pattern FROM_PATTERN = Pattern.compile("(^From: )(.*)",
            Pattern.CASE_INSENSITIVE);
    private static final EmailAddressParser EMAIL_ADDRESS_PARSER = new EmailAddressParser();

    @Inject
    public FromAddressParser() {
        super("from", FROM_PATTERN);
    }

    @Override
    protected void process(final String groupTwo, final Header header) {
        header.setFrom(EMAIL_ADDRESS_PARSER.parse(groupTwo));
    }
}
