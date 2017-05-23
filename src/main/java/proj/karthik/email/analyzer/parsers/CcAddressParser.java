package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import java.util.List;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.model.EmailAddress;
import proj.karthik.email.analyzer.model.Header;

/**
 * Parse the "cc" address from the email document
 * Expected line format: "Cc: shelley.corman@enron.com, karen.denne@enron.com"
 */
public class CcAddressParser extends ToAddressParser {
    private static final Pattern CC_PATTERN = Pattern.compile("(^Cc: )(.*)",
            Pattern.CASE_INSENSITIVE);

    @Inject
    public CcAddressParser() {
        super("cc", CC_PATTERN);
    }

    @Override
    protected void setMetadataWith(final Header header, final List<EmailAddress> emailAddresses) {
        header.setCc(emailAddresses);
    }
}
