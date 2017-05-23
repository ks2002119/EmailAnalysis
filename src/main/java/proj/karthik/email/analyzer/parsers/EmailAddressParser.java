package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.model.EmailAddress;

/**
 * Parse email address string into {@link EmailAddress} object
 */
public class EmailAddressParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmailAddressParser.class);
    // Mail::RFC822 compliant email address
    // Credits: http://www.mkyong.com/regular-expressions/how-to-validate-email-address-with
    // -regular-expression/
    private static final Pattern EMAIL_PATTERN = Pattern.compile(
            "(\\b[A-Z0-9._%+-]+)(@)([A-Z0-9.-]+\\.[A-Z]{2,4}\\b)", Pattern.CASE_INSENSITIVE);

    @Inject
    public EmailAddressParser() {
    }

    public EmailAddress parse(final String input) {
        LOGGER.debug("Applying email address parsing rule to {}", input);
        Matcher matcher = EMAIL_PATTERN.matcher(input);
        String id;
        String domain;
        // Email consists of 3 parts - id, character '@', domain (xyz.com)
        if (matcher.find() && matcher.groupCount() == 3) {
            id = matcher.group(1); // User id
            domain = matcher.group(3); // Domain
        } else {
            throw new AnalyzerException("%s is not a parseable email address", input);
        }
        return new EmailAddress(id, domain);
    }
}
