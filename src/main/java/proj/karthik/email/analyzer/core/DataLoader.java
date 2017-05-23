package proj.karthik.email.analyzer.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import proj.karthik.email.analyzer.model.Email;

/**
 * Analyzes the email corpus and extracts the metadata.
 */
public class DataLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataLoader.class);

    private final ObjectMapper objectMapper;
    private final EmailParserFactory extractorFactory;

    @Inject
    public DataLoader(ObjectMapper objectMapper, EmailParserFactory extractorFactory) {
        this.objectMapper = objectMapper;
        this.extractorFactory = extractorFactory;
    }

    public void load(String dataPath, String emailTableFile) {
        Path dataDirPath = Paths.get(dataPath);
        if (Files.exists(dataDirPath)) {
            LOGGER.info("Loading data from {}", dataPath);
            try (BufferedWriter bufferedWriter = new BufferedWriter(
                    new FileWriter(emailTableFile))) {
                Stream<Path> pathStream = Files.walk(dataDirPath, FileVisitOption.FOLLOW_LINKS);
                pathStream.filter(path -> path.toFile().isFile() &&
                        path.toFile().getName().endsWith("txt"))
                        .map(path -> extractorFactory.create(path))
                        .forEach(emailParser -> {
                            try {
                                Email email = emailParser.parse();
                                writeToStore(emailTableFile, bufferedWriter, email);
                            } catch (IOException e) {
                                LOGGER.error("Error parsing email file: " + emailParser.getPath(),
                                        e);
                            }
                        });
            } catch (IOException e) {
                throw new AnalyzerException(e, "Error writing parsed email document to %s",
                        emailTableFile);
            } catch (DirectoryIteratorException e) {
                throw new AnalyzerException(e, "Error during directory iteration");
            }
            LOGGER.info("Data successfully loaded");
        } else {
            throw new AnalyzerException("Data directory %s does not exist", dataPath);
        }
    }

    private void writeToStore(String emailTableFile, BufferedWriter bufferedWriter,
            Email email) {
        try {
            String jsonData = objectMapper.writeValueAsString(email);
            bufferedWriter.write(jsonData + "\n");
        } catch (JsonProcessingException e) {
            LOGGER.warn("Error converting email to json string", e);
        } catch (IOException e) {
            LOGGER.warn(String.format("Error writing json data to %s", emailTableFile), e);
        }
    }
}
