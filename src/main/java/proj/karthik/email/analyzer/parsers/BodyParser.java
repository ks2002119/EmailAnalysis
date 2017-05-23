package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.core.EmailAttribute;
import proj.karthik.email.analyzer.core.Parser;
import proj.karthik.email.analyzer.core.ParserFactory;
import proj.karthik.email.analyzer.model.Body;
import proj.karthik.email.analyzer.model.Header;

/**
 * Parser rule for email setBody.
 */
public class BodyParser implements Parser<String, Body> {

    private static final Pattern PATTERN_LINE = Pattern.compile("\\R");
    private static final Pattern PATTERN_EMBEDDED_EMAIL = Pattern.compile("-*Original " +
             "Message-*\\R", Pattern.CASE_INSENSITIVE);
    private static final Logger LOGGER = LoggerFactory.getLogger(BodyParser.class);
    private final ParserFactory parserFactory;

    @Inject
    public BodyParser(ParserFactory parserFactory) {
        this.parserFactory = parserFactory;
    }

    @Override
    public void parse(final String input, final Body body) {
        List<Parser<String, Header>> embeddedParsers = getParsers();
        StringBuilder bodyMessage = new StringBuilder();
        final boolean[] encounteredEmbeddedEmail = {false};
        final boolean[] parsedEmbeddedHeader = {false};
        Header embeddedHeader = new Header();
        Arrays.stream(PATTERN_LINE.split(input))
                .forEach(line -> {
                    if (StringUtils.isNotBlank(line)) {
                        if (encounteredEmbeddedEmail[0] &&
                                !parsedEmbeddedHeader[0]) {
                            if (embeddedParsers.isEmpty()) {
                                parsedEmbeddedHeader[0] = true;
                                bodyMessage.append(line);
                            } else {
                                Parser<String, Header> parser = embeddedParsers.remove(0);
                                try {
                                    parser.parse(line, embeddedHeader);
                                } catch (Exception e) {
                                    LOGGER.warn("Unable to match embedded parser: " + parser.toString
                                            (), e);
                                }
                            }
                            embeddedParsers.stream().forEach(parser -> {
                                if (parser.matches(line)){
                                    parser.parse(line, embeddedHeader);
                                }
                            });
                        }
                        if (!PATTERN_EMBEDDED_EMAIL.matcher(line).find()) {
                            bodyMessage.append(line);
                        } else {
                            encounteredEmbeddedEmail[0] = true;
                        }
                    } else {
                        bodyMessage.append(line);
                    }
                });
        body.setText(bodyMessage.toString());
    }

    private List<Parser<String, Header>> getParsers() {
        List<Parser<String, Header>> embeddedParsers = new ArrayList<>(4);
        embeddedParsers.add(parserFactory.getParser(EmailAttribute.FROM));
        embeddedParsers.add(parserFactory.getParser(EmailAttribute.SENT_DATE));
        embeddedParsers.add(parserFactory.getParser(EmailAttribute.TO));
        embeddedParsers.add(parserFactory.getParser(EmailAttribute.SUBJECT));
        return embeddedParsers;
    }

    @Override
    public boolean matches(final String input) {
        return true;
    }
}
