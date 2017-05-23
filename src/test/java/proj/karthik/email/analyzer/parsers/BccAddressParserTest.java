package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import proj.karthik.email.analyzer.model.EmailAddress;
import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link BccAddressParser}
 */
public class BccAddressParserTest {
    private static final BccAddressParser BCC_ADDRESS_PARSER = new BccAddressParser();

    @Test
    public void testBccAddressParsing() throws Exception {
        Header header = new Header();
        BCC_ADDRESS_PARSER.parse("Bcc: michael.terraso@enron.com, karen.denne@enron.com", header);
        assertEquals(new EmailAddress("michael.terraso", "enron.com"), header.bcc().get(0));
        assertEquals(new EmailAddress("karen.denne", "enron.com"), header.bcc().get(1));
    }

    @Test
    public void testBccAddressParsingError() throws Exception {
        Header header = new Header();
        try {
            BCC_ADDRESS_PARSER.parse("steven.kean@enron.com", header);
        } catch (Exception e) {
            assertEquals("Unable to match pattern (^Bcc: )(.*) for input:steven.kean@enron" +
                     ".com", e.getMessage());
        }
    }
}
