package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import java.util.List;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.model.EmailAddress;
import proj.karthik.email.analyzer.model.Header;

/**
 * Parse the "bcc" address from the email document
 * Expected line format: "Bcc: shelley.corman@enron.com, karen.denne@enron.com"
 */
public class BccAddressParser extends ToAddressParser {
    private static final Pattern BCC_PATTERN = Pattern.compile("(^Bcc: )(.*)",
            Pattern.CASE_INSENSITIVE);

    @Inject
    public BccAddressParser() {
        super("bcc", BCC_PATTERN);
    }

    @Override
    protected void setMetadataWith(Header header, List<EmailAddress> emailAddresses) {
        header.setBcc(emailAddresses);
    }
}
