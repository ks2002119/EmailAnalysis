package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.core.Parser;
import proj.karthik.email.analyzer.model.Header;

/**
 * Parse the content type as string from the email document
 * Expected line format: "Content-Type: text/plain; charset=us-ascii"
 */
public class ContentTypeParser implements Parser<String, Header> {
    private static final Pattern CONTENT_TYPE_PATTERN = Pattern.compile("(^Content-Type:)(.*;)" +
                     "(.*charset=)(.*)", Pattern.CASE_INSENSITIVE);

    @Inject
    public ContentTypeParser() {
    }

    @Override
    public void parse(final String input, final Header header) {
        Matcher matcher = CONTENT_TYPE_PATTERN.matcher(input);
        if (matcher.find()) {
            String groupTwo = matcher.group(2);
            groupTwo = groupTwo.substring(0, groupTwo.length() - 1);
            header.setContentType(groupTwo.trim());
            header.setCharset(matcher.group(4).trim());
        } else {
            throw new AnalyzerException("Unable to match pattern: %s", CONTENT_TYPE_PATTERN
                    .toString());
        }
    }

    @Override
    public boolean matches(final String input) {
        return CONTENT_TYPE_PATTERN.matcher(input).find();
    }
}
