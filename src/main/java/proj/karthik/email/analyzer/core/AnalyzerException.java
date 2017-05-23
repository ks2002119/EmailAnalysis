package proj.karthik.email.analyzer.core;

/**
 * Exception used in the analyzer module.
 */
public class AnalyzerException extends RuntimeException {

    public AnalyzerException(final String message) {
        super(message);
    }

    public AnalyzerException(final String message, final String... args) {
        super(String.format(message, (Object[]) args));
    }

    public AnalyzerException(Throwable throwable, String message) {
        super(message, throwable);
    }

    public AnalyzerException(Throwable throwable, String message, String... args) {
        this(throwable, String.format(message, (Object[]) args));
    }
}
