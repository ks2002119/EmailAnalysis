package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import proj.karthik.email.analyzer.model.EmailAddress;
import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link FromAddressParser}
 */
public class FromAddressParserTest {
    private static final FromAddressParser FROM_ADDRESS_PARSER = new FromAddressParser();

    @Test
    public void testFromAddressParsing() throws Exception {
        Header header = new Header();
        FROM_ADDRESS_PARSER.parse("From: steven.kean@enron.com", header);
        assertEquals(new EmailAddress("steven.kean", "enron.com"), header.from());
    }

    @Test
    public void testFromAddressParsingError() throws Exception {
        Header header = new Header();
        try {
            FROM_ADDRESS_PARSER.parse("steven.kean@enron.com", header);
        } catch (Exception e) {
            assertEquals("Unable to match pattern (^From: )(.*) for input:steven.kean@enron" +
                     ".com", e.getMessage());
        }
    }
}
