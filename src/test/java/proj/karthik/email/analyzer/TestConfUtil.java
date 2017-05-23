package proj.karthik.email.analyzer;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.core.ParserModule;
import proj.karthik.email.analyzer.core.TestModule;

/**
 * Test configuration utility.
 */
public class TestConfUtil {
    private static final Map<String, Object> CONF_MAP = new HashMap<>();
    private static String DATA_PATH;
    private static String EMAIL_STORE_PATH;

    static {
        DATA_PATH = Paths.get(".", "src", "test", "resources", "data_files", "valid").normalize()
                .toAbsolutePath().toString();
        try {
            File tempFile = File.createTempFile("test", "email");
            EMAIL_STORE_PATH = tempFile.getAbsolutePath().toString();
        } catch (IOException e) {
            throw new AnalyzerException(e, "Error creating temp file for storing emails");
        }
        CONF_MAP.put("data", DATA_PATH);
        Map<String, Object> nested = new HashMap<>();
        nested.put("confs", new LinkedList<String>());
        CONF_MAP.put("spark", nested);
    }
    private static final Injector injector = Guice.createInjector(new ParserModule(),
            new TestModule(ConfigFactory.load().withFallback(ConfigFactory.parseMap(CONF_MAP)),
            DATA_PATH, EMAIL_STORE_PATH));

    public static Injector getInjector() {
        return injector;
    }
}
