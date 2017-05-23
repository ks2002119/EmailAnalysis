package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import proj.karthik.email.analyzer.model.EmailAddress;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link EmailAddressParser}
 */
public class EmailAddressParserTest {

    private static final EmailAddressParser EMAIL_PARSER = new EmailAddressParser();

    @Test
    public void testValidEmail() throws Exception {
        String email = "xyz@foo.com";
        EmailAddress parsedEmailAddress = EMAIL_PARSER.parse(email);
        assertEquals(new EmailAddress("xyz", "foo.com"), parsedEmailAddress);
    }

    @Test
    public void testInvalidEmail() throws Exception {
        String email = "xyz.@foo";
        try {
            EMAIL_PARSER.parse(email);
        } catch (Exception e) {
            assertEquals("xyz.@foo is not a parseable email address", e.getMessage());
        }
    }
}
