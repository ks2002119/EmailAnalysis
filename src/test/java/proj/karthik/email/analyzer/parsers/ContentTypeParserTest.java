package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link AttachmentParser}
 */
public class ContentTypeParserTest {
    private static final ContentTypeParser CONTENT_TYPE_PARSER = new ContentTypeParser();

    @Test
    public void testContentTypeParsing() throws Exception {
        Header header = new Header();
        String input = "Content-Type: text/plain; charset=us-ascii";
        assertTrue(CONTENT_TYPE_PARSER.matches(input));
        CONTENT_TYPE_PARSER.parse(input, header);
        assertEquals("text/plain", header.getContentType());
        assertEquals("us-ascii", header.getCharset());
    }

    @Test
    public void testContentTypeParsingError() throws Exception {
        Header header = new Header();
        try {
            CONTENT_TYPE_PARSER.parse("text/plain; charset=us-ascii", header);
        } catch (Exception e) {
            assertEquals("Unable to match pattern: (^Content-Type:)(.*;)(.*charset=)(.*)",
                    e.getMessage());
        }
    }
}
