package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link MessageIdParser}
 */
public class MessageIdParserTest {
    private static final MessageIdParser MESSAGE_ID_PARSER = new MessageIdParser();

    @Test
    public void testMessageIdParsing() throws Exception {
        Header header = new Header();
        MESSAGE_ID_PARSER.parse("Message-ID: <29023172.1075847627587.JavaMail.evans@thyme>", header);
        assertEquals("<29023172.1075847627587.JavaMail.evans@thyme>", header.id());
    }

    @Test
    public void testMessageIdParsingError() throws Exception {
        Header header = new Header();
        try {
            MESSAGE_ID_PARSER.parse("<29023172.1075847627587.JavaMail.evans@thyme>", header);
        } catch (Exception e) {
            assertEquals("Unable to match pattern (^Message-ID: )(.*) for " +
                     "input:<29023172.1075847627587.JavaMail.evans@thyme>", e.getMessage());
        }
    }
}
