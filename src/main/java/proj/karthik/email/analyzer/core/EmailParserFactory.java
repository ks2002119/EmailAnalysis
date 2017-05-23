package proj.karthik.email.analyzer.core;

import java.nio.file.Path;

/**
 * Factory interface to help with assisted injection.
 */
public interface EmailParserFactory {

    /**
     * Creates a {@link EmailParser} for the given path.
     *
     * @param path
     * @return metadataExtractor
     */
    EmailParser create(Path path);
}
