package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.model.Header;
import proj.karthik.email.analyzer.model.Subject;

/**
 * Parse the subject as string from the email document.
 * Expected line format: "Subject: Ken Lay's email to Sen. Brulte"
 */
public class SubjectParser extends BaseTwoPartParser<Header> {
    private static final Pattern SUBJECT_PATTERN = Pattern.compile("(^Subject: )(.*)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern FWD_PATTERN = Pattern.compile("(^Fw:)(.*)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern RE_PATTERN = Pattern.compile("(^Fw:)(.*)",
            Pattern.CASE_INSENSITIVE);

    @Inject
    public SubjectParser() {
        super("subject", SUBJECT_PATTERN);
    }

    @Override
    public void process(final String input, final Header header) {
        String lowerCase = input.toLowerCase().trim();
        header.setSubject(new Subject(strip(input), lowerCase.startsWith("fw:"),
                lowerCase.startsWith("re:")));
    }

    private String strip(final String input) {
        Matcher fwdMatcher = FWD_PATTERN.matcher(input);
        if (fwdMatcher.find()) {
            return fwdMatcher.group(2);
        } else {
            Matcher replyMatcher = RE_PATTERN.matcher(input);
            if (replyMatcher.find()) {
                return replyMatcher.group(2);
            }
        }
        return input;
    }
}
