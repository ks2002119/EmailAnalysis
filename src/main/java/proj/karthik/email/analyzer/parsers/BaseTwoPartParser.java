package proj.karthik.email.analyzer.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.core.Parser;

/**
 * Base class for all the parsers that has two parts in the line.
 */
public abstract class BaseTwoPartParser<P> implements Parser<String, P> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseTwoPartParser.class);
    private final Pattern pattern;
    private final String name;

    public BaseTwoPartParser(String name, Pattern pattern) {
        this.name = name;
        this.pattern = pattern;
    }

    @Override
    public boolean matches(final String input) {
        Matcher matcher = getMatcher(input);
        return matcher.find() && matcher.groupCount() == 2;
    }

    @Override
    public void parse(final String input, P part) {
        LOGGER.debug("Applying rule:{} to input:{}", name, input);
        Matcher matcher = getMatcher(input);
        if (matcher.find() && matcher.groupCount() == 2) {
            // Since the start of the line matches, lets strip the key to get the value.
            String groupTwo = matcher.group(2);
            process(groupTwo, part);
        } else {
            throw new AnalyzerException("Unable to match pattern %s for input:%s",
                    pattern.toString(), input);
        }
    }

    protected abstract void process(final String groupTwo, final P header);

    //----------------------------------- Private Methods ---------------------------------------//
    private Matcher getMatcher(final String input) {
        return this.pattern.matcher(input);
    }
}
