package proj.karthik.email.analyzer.core;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import proj.karthik.email.analyzer.model.Email;
import proj.karthik.email.analyzer.parsers.ParserContext;

/**
 * Parses the given email document and create a model representation.
 */
public class EmailParserImpl implements EmailParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailParserImpl.class);
    private final Path path;
    private final ParserContext parserContext;

    @AssistedInject
    public EmailParserImpl(ParserContext parserContext, @Assisted Path path) {
        this.parserContext = parserContext;
        this.path = path;
    }

    @Override
    public Email parse() throws IOException {
        LOGGER.info("Processing email document: {}", path.toString());
        Files.lines(path)
                .forEach(line -> parserContext.consume(line));
        return parserContext.getEmail();
    }

    @Override
    public Path getPath() {
        return path;
    }
}
