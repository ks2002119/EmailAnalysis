package proj.karthik.email.analyzer.core;

import java.io.IOException;
import java.nio.file.Path;

import proj.karthik.email.analyzer.model.Email;

/**
 * Created by ksubramanian on 3/3/17.
 */
public interface EmailParser {
    Email parse() throws IOException;

    Path getPath();
}
