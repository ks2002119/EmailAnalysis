package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import java.util.regex.Pattern;

import proj.karthik.email.analyzer.model.Header;

/**
 * Parse the attachment file name as string from the email document
 * Expected line format: "X-FileName: LBLAIR (Non-Privileged).pst"
 */
public class AttachmentParser extends BaseTwoPartParser<Header> {
    private static final Pattern MESSAGE_ID_PATTERN = Pattern.compile("(^X-FileName: )(.*)",
            Pattern.CASE_INSENSITIVE);

    @Inject
    public AttachmentParser() {
        super("attachment", MESSAGE_ID_PATTERN);
    }

    @Override
    public void process(final String input, final Header header) {
        header.setAttachmentName(input);
    }
}
