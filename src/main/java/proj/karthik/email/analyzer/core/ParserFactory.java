package proj.karthik.email.analyzer.core;

/**
 * Factory used to create parsers for various attributes within an email.
 */
@FunctionalInterface
public interface ParserFactory {

    /**
     * Returns the parser for the given email component.
     *
     * @param emailAttribute
     * @return parser
     */
    Parser getParser(EmailAttribute emailAttribute);
}
