package proj.karthik.email.analyzer.core;

import com.google.inject.Key;
import com.google.inject.name.Names;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;

import proj.karthik.email.analyzer.TestConfUtil;
import proj.karthik.email.analyzer.model.Body;
import proj.karthik.email.analyzer.model.Email;
import proj.karthik.email.analyzer.model.EmailAddress;
import proj.karthik.email.analyzer.model.Header;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit test for {@link EmailParserImpl}
 */
public class EmailParserImplTest {

    private static EmailParserFactory extractorFactory;

    @BeforeClass
    public static void setUp() throws Exception {
        extractorFactory = TestConfUtil.getInjector().getInstance(EmailParserFactory.class);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        String emailTablePath = TestConfUtil.getInjector().getInstance(
                Key.get(String.class, Names.named(CoreModule.EMAIL_TABLE_PATH)));
        Path emailTable = Paths.get(emailTablePath);
        if (emailTable.toFile().exists()) {
            Files.delete(emailTable);
        }
    }


    @Test
    public void testMetadataExtractor() throws Exception {
        Path path = Paths.get(getClass().getResource("/data_files/valid/test_with_cc_n_bcc.txt")
                .toURI());
        EmailParser emailParser = extractorFactory.create(path);
        Email email = emailParser.parse();
        Header header = email.getHeader();
        assertEquals("<5140735.1075840386341.JavaMail.evans@thyme>", header.id());
        assertEquals(ZonedDateTime.parse("2001-12-13T15:21-08:00[America/Los_Angeles]").toInstant
                        ().toEpochMilli(), header.timestamp().toInstant().toEpochMilli());
        assertEquals("Confidential communications", header.subject().getText());
        assertEquals(new EmailAddress("richardson", "copn.com"), header.from());
        assertEquals(16, header.to().size());
        assertEquals(2, header.cc().size());
        assertEquals(2, header.bcc().size());
        assertEquals(951, email.getBody().getText().length());
    }

    @Test
    public void testMetadataExtractorWithNoMatch() throws Exception {
        Path path = Paths.get(getClass().getResource("/data_files/invalid/test_with_no_match.txt")
                .toURI());
        EmailParser emailParser = extractorFactory.create(path);
        Email email = emailParser.parse();
        assertNotNull(email.getHeader());
    }

    @Test
    public void testComplexEmailExtraction() throws Exception {
        Path path = Paths.get(EmailParserImplTest.class.getResource
                 ("/data_files/valid/complex_email_1.txt").toURI());
        EmailParser emailParser = extractorFactory.create(path);
        Email email = emailParser.parse();
        Header header = email.getHeader();
        assertEquals("<12891771.1075840784712.JavaMail.evans@thyme>", header.id());
        Body body = email.getBody();
        assertEquals(4357, body.getText().length());
    }

    @Test
    public void testComplexEmailExtraction_2() throws Exception {
        Path path = Paths.get(EmailParserImplTest.class.getResource
                ("/data_files/valid/106296.txt").toURI());
        EmailParser emailParser = extractorFactory.create(path);
        Email email = emailParser.parse();
        Header header = email.getHeader();
        assertEquals("<11991339.1075842536086.JavaMail.evans@thyme>", header.id());
    }

    @Test
    public void testEmailExtractionWithReply() throws Exception {
        Path path = Paths.get(EmailParserImplTest.class.getResource
                ("/data_files/valid/reply_email.txt").toURI());
        EmailParser emailParser = extractorFactory.create(path);
        Email email = emailParser.parse();
        Header header = email.getHeader();
        assertEquals("<14668250.1075863426651.JavaMail.evans@thyme>", header.id());
        assertEquals(true, email.getHeader().subject().isReply());
        Body body = email.getBody();
        assertEquals(1171, body.getText().length());
    }
}
