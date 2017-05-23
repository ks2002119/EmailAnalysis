package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import proj.karthik.email.analyzer.model.EmailAddress;
import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link CcAddressParser}
 */
public class CcAddressParserTest {
    private static final CcAddressParser CC_ADDRESS_PARSER = new CcAddressParser();

    @Test
    public void testCcAddressParsing() throws Exception {
        Header header = new Header();
        CC_ADDRESS_PARSER.parse("Cc: michael.terraso@enron.com, karen.denne@enron.com", header);
        assertEquals(new EmailAddress("michael.terraso", "enron.com"), header.cc().get(0));
        assertEquals(new EmailAddress("karen.denne", "enron.com"), header.cc().get(1));
    }

    @Test
    public void testCcAddressParsingError() throws Exception {
        Header header = new Header();
        try {
            CC_ADDRESS_PARSER.parse("steven.kean@enron.com", header);
        } catch (Exception e) {
            assertEquals("Unable to match pattern (^Cc: )(.*) for input:steven.kean@enron" +
                     ".com", e.getMessage());
        }
    }
}
