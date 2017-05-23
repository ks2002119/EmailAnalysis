package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import proj.karthik.email.analyzer.model.EmailAddress;
import proj.karthik.email.analyzer.model.Header;

/**
 * Parse the "to" address from the email document
 * Expected line format: "To: michael.terraso@enron.com, karen.denne@enron.com"
 */
public class ToAddressParser extends BaseTwoPartParser<Header> {
    private static final Pattern TO_PATTERN = Pattern.compile("(^To: )(.*)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern COMMA_PATTERN = Pattern.compile(",");
    private static final EmailAddressParser EMAIL_ADDRESS_PARSER = new EmailAddressParser();

    @Inject
    public ToAddressParser() {
        super("to", TO_PATTERN);
    }

    public ToAddressParser(final String name, final Pattern pattern) {
        super(name, pattern);
    }

    @Override
    protected void process(final String groupTwo, final Header header) {
        String[] emailAddresses = COMMA_PATTERN.split(groupTwo);
        List<EmailAddress> toAddresses = Arrays.stream(emailAddresses)
                .map(emailStr -> EMAIL_ADDRESS_PARSER.parse(emailStr.trim()))
                .collect(Collectors.toList());
        setMetadataWith(header, toAddresses);
    }

    protected void setMetadataWith(Header header, List<EmailAddress> emailAddresses) {
        header.setTo(emailAddresses);
    }
}
