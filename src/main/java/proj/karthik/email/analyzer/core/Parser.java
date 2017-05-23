package proj.karthik.email.analyzer.core;

/**
 * Parser that takes an input of type I and parses it as type O.
 *
 * @param <I> Input for the parser
 * @param <P> Part of the email to use with this parser
 */
public interface Parser<I, P> {

    /**
     * Parses the given input.
     * @param input
     * @param component
     * @return output
     */
    void parse(I input, P component);

    /**
     * Returns true if this parser can handle the given input.
     * @param input
     * @return true if match found
     */
    boolean matches(String input);
}
