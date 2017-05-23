package proj.karthik.email.analyzer.parsers;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.core.EmailAttribute;
import proj.karthik.email.analyzer.core.Parser;
import proj.karthik.email.analyzer.core.ParserFactory;
import proj.karthik.email.analyzer.model.Body;
import proj.karthik.email.analyzer.model.Email;
import proj.karthik.email.analyzer.model.Header;

/**
 * This class maintains the state of the email document parsing session.
 * This class also contains the order of the rules for parsing email documents.
 * Logic:
 *  1. Applies the parser rules in order configured to minimize unnecessary matching efforts
 *  2. Supports email attributes spanning multiple line
 *  3. Handles empty lines as the end of an attribute.
 *  4. When encountering unmatched lines, skips over it to be fault-tolerant
 */
public class ParserContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParserContext.class);

    private static final String CONF_ANALYZER_PARSERS_PARSER_ORDER = "analyzer.parsers" +
             ".parser_order";
    private static final String CONF_ANALYZER_PARSERS_SKIP_PATTERN = "analyzer.parsers" +
            ".skip_pattern";
    private static final String CONF_ANALYZER_PARSERS_KEYWORDS= "analyzer.parsers.keywords";

    private final List<Parser> parsers = new ArrayList<>(10);
    private final List<Pattern> skipPatterns;
    private final Header header = new Header();
    private final Body body = new Body();
    private final Parser<String, Body> bodyParser;
    private final List<Pattern> keywords;

    private StringBuilder data;
    private Parser parserInProgress;
    private State state = State.BEGIN_HEADER;

    @Inject
    public ParserContext(Config config, ParserFactory parserFactory) {
        this.bodyParser = parserFactory.getParser(EmailAttribute.BODY);
        List<String> parserOrder = config.getStringList(CONF_ANALYZER_PARSERS_PARSER_ORDER);
        if (parserOrder == null || parserOrder.isEmpty()) {
            // Lets use default parser order here
            parsers.add(parserFactory.getParser(EmailAttribute.ID));
            parsers.add(parserFactory.getParser(EmailAttribute.DATE));
            parsers.add(parserFactory.getParser(EmailAttribute.FROM));
            parsers.add(parserFactory.getParser(EmailAttribute.TO));
            parsers.add(parserFactory.getParser(EmailAttribute.SUBJECT));
            parsers.add(parserFactory.getParser(EmailAttribute.CC));
            parsers.add(parserFactory.getParser(EmailAttribute.CONTENT_TYPE));
            parsers.add(parserFactory.getParser(EmailAttribute.BCC));
            parsers.add(parserFactory.getParser(EmailAttribute.ATTACHMENT));
        } else {
            parserOrder.forEach(name ->
                    parsers.add(parserFactory.getParser(EmailAttribute.from(name))));
        }
        List<String> skipPatterns = config.getStringList(CONF_ANALYZER_PARSERS_SKIP_PATTERN);
        if (skipPatterns != null) {
            this.skipPatterns = skipPatterns.stream()
                    .map(pattern -> Pattern.compile(pattern, Pattern.CASE_INSENSITIVE))
                    .collect(Collectors.toList());
        } else {
            this.skipPatterns = new LinkedList<>();
        }
        List<String> keywords = config.getStringList(CONF_ANALYZER_PARSERS_KEYWORDS);
        if (keywords != null) {
            this.keywords = keywords.stream()
                    .map(pattern -> Pattern.compile(pattern, Pattern.CASE_INSENSITIVE))
                    .collect(Collectors.toList());
        } else {
            this.keywords = new LinkedList<>();
        }
    }

    /**
     * Consume a line and apply the parser rule.
     *
     * @param line
     */
    public void consume(final String line) {
        switch (state) {
            case BEGIN_HEADER:
                if (StringUtils.isBlank(line)) {
                    LOGGER.debug("Empty line encountered");
                    endHeader();
                } else {
                    consumeHeader(line);
                }
                return;
            case END_HEADER:
                if (StringUtils.isNotBlank(line)) {
                    // Lets first try keywords
                    Optional<Pattern> keywordMatch = keywords.stream()
                            .filter(pattern -> pattern.matcher(line).find())
                            .findAny();
                    if (keywordMatch.isPresent()) {
                        LOGGER.debug("Skipping line {} starting with keyword: {}", line,
                                keywordMatch.get().toString());
                        return;
                    } else {
                        // Lets check for skip pattern.
                        Optional<Pattern> skipPattern = skipPatterns.stream()
                                .filter(pattern -> pattern.matcher(line).find())
                                .findAny();
                        if (skipPattern.isPresent()) {
                            LOGGER.debug("Skipping line {} starting with skipword: {}", line,
                                    skipPattern.get().toString());
                            finishPendingParseRule();
                            return;
                        } else {
                            startBody();
                            // Didn't match skip pattern or other keyword. Lets move to start of body.
                            if (data == null) {
                                data = new StringBuilder();
                                data.append(line);
                            } else {
                                data.append("\n")
                                        .append(line);
                            }
                        }
                    }
                }
                return;
            case BEGIN_BODY:
            default:
                if (data.length() != 0) {
                    data.append("\n");
                }
                data.append(line);
        }
    }

    /**
     * Returns the parsed email.
     *
     * @return parsedEmail
     */
    public Email getEmail() {
        if (data != null) {
            this.bodyParser.parse(data.toString(), body);
            clearStateVariables();
        }
        return new Email(header, body);
    }

    //------------------------------------ Private Methods ---------------------------------------//

    private void endHeader() {
        finishPendingParseRule();
        clearStateVariables();
        endHeaderState();
    }

    private void startBody() {
        if (state == State.END_HEADER) {
            state = State.BEGIN_BODY;
        } else {
            // Something is wrong here.
            LOGGER.warn("Incompatible state transition {} to {}", state.toString(),
                    State.BEGIN_BODY.toString());
            state = State.BEGIN_BODY;
        }
    }

    private void clearStateVariables() {
        parserInProgress = null;
        data = null;
    }

    private void endHeaderState() {
        if (state == State.BEGIN_HEADER) {
            LOGGER.debug("Transitioning from BEGIN_HEADER to END_HEADER state");
            // Lets transition to BEGIN_BODY state
            state = State.END_HEADER;
        }
    }

    private void consumeHeader(final String line) {
        if (!matchesSkipPattern(line)) {
            applyHeaderRules(line);
        } else {
            finishPendingParseRule();
        }
    }

    private boolean matchesSkipPattern(final String line) {
        Optional<Pattern> skipPatternMatch = skipPatterns.stream()
                    .filter(pattern -> pattern.matcher(line).find())
                    .findAny();
        boolean skip = skipPatternMatch.isPresent();
        if (skip) {
            LOGGER.debug("Skip pattern {} matched by {}", skipPatternMatch.get().toString(), line);
        }
        return skip;
    }

    private void applyHeaderRules(final String line) {
        Optional<Parser> matchingParser = parsers.stream()
                .filter(parser -> parser.matches(line))
                .findFirst();
        if (matchingParser.isPresent()) {
            // A new match is found. Lets check if we had any other parser in progress already.
            finishPendingParseRule();
            setNewParserRule(line, matchingParser.get());
        } else if (parserInProgress != null && data != null) {
            // Check if we had a parser session in progress
            LOGGER.debug("Adding {} to existing parser session", line);
            data.append(line);
        } else {
            // nothing in progress. Unidentified line.
            throw new AnalyzerException("Unable to apply parser rules to line: %s", line);
        }
    }

    private void finishPendingParseRule() {
        if (parserInProgress != null && data != null) {
            LOGGER.debug("Finishing parser rule: {}", parserInProgress.toString());
            parserInProgress.parse(data.toString(), header);
            data = new StringBuilder();
            parserInProgress = null;
        }
    }

    private void setNewParserRule(final String line, Parser parser) {
        LOGGER.debug("Setting new parser rule: {} starting from line: {}", parser, line);
        parserInProgress = parser;
        data = new StringBuilder(line);
        // We have already applied this parser. Lets remove it.
        // Assumption: We are not going to see multiple lines for same email attribute.
        parsers.remove(parser);
    }

    private enum State {
        // Header appears at the beginning of the email.
        BEGIN_HEADER,
        END_HEADER,
        // Body follows the email header after an empty line.
        BEGIN_BODY
    }
}
