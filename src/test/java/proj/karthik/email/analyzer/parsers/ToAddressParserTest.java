package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import proj.karthik.email.analyzer.model.EmailAddress;
import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link ToAddressParser}
 */
public class ToAddressParserTest {
    private static final ToAddressParser TO_ADDRESS_PARSER = new ToAddressParser();

    @Test
    public void testToAddressParsing() throws Exception {
        Header header = new Header();
        TO_ADDRESS_PARSER.parse("To: michael.terraso@enron.com, karen.denne@enron.com", header);
        assertEquals(new EmailAddress("michael.terraso", "enron.com"), header.to().get(0));
        assertEquals(new EmailAddress("karen.denne", "enron.com"), header.to().get(1));
    }

    @Test
    public void testToAddressParsingError() throws Exception {
        Header header = new Header();
        try {
            TO_ADDRESS_PARSER.parse("steven.kean@enron.com", header);
        } catch (Exception e) {
            assertEquals("Unable to match pattern (^To: )(.*) for input:steven.kean@enron" +
                     ".com", e.getMessage());
        }
    }
}
