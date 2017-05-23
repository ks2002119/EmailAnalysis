package proj.karthik.email.analyzer.parsers;

import org.junit.Test;

import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link SubjectParser}
 */
public class SubjectParserTest {
    private static final SubjectParser SUBJECT_PARSER = new SubjectParser();

    @Test
    public void testSubjectParsing() throws Exception {
        Header header = new Header();
        SUBJECT_PARSER.parse("Subject: Ken Lay's email to Sen. Brulte", header);
        assertEquals("Ken Lay's email to Sen. Brulte", header.subject().getText());
    }

    @Test
    public void testSubjectParsingError() throws Exception {
        Header header = new Header();
        try {
            SUBJECT_PARSER.parse("Ken Lay's email to Sen. Brulte", header);
        } catch (Exception e) {
            assertEquals("Unable to match pattern (^Subject: )(.*) for input:Ken Lay's email to " +
                     "Sen. Brulte", e.getMessage());
        }
    }
}
