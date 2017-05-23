package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link AttachmentParser}
 */
public class AttachmentParserTest {
    private static final AttachmentParser ATTACHMENT_PARSER = new AttachmentParser();

    @Test
    public void testAttachmentParsing() throws Exception {
        Header header = new Header();
        ATTACHMENT_PARSER.parse("X-FileName: LBLAIR (Non-Privileged).pst", header);
        assertEquals("LBLAIR (Non-Privileged).pst", header.getAttachmentName());
    }

    @Test
    public void testAttachmentParsingError() throws Exception {
        Header header = new Header();
        try {
            ATTACHMENT_PARSER.parse("LBLAIR (Non-Privileged).pst", header);
        } catch (Exception e) {
            assertEquals("Unable to match pattern (^X-FileName: )(.*) for input:LBLAIR " +
                     "(Non-Privileged).pst", e.getMessage());
        }
    }
}
