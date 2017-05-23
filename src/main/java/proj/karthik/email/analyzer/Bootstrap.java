package proj.karthik.email.analyzer;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import proj.karthik.email.analyzer.core.AnalyzerException;
import proj.karthik.email.analyzer.core.CoreModule;
import proj.karthik.email.analyzer.core.DataLoader;
import proj.karthik.email.analyzer.core.ParserModule;
import proj.karthik.email.analyzer.report.PrebuildReportType;
import proj.karthik.email.analyzer.report.ReportFactory;
import proj.karthik.email.analyzer.util.DatasetUtil;

/**
 * Entry point into the application
 */
public class Bootstrap {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);
    private static final String DATA = "data";
    private static final String TABLE_EMAILS = "emails";
    private static final String PATH_DOT = ".";
    private static final String EMAIL_JSON = "%s.json";
    private static final Logger CONSOLE_LOG = LoggerFactory.getLogger("Console");
    private static final Pattern PATTERN_EXEC = Pattern.compile("^(exec )(.*)", Pattern
            .CASE_INSENSITIVE);
    private static final Options OPTIONS = new Options();
    private static final Pattern PATTERN_REPORT = Pattern.compile("^(report )(.*)");
    private static final Pattern PATTERN_HELP = Pattern.compile("^(help )*");

    static {
        OPTIONS.addOption(DATA, true, "Location of the data directory. Point to the " +
                "root directory where the email corpus is extracted.");
        OPTIONS.addOption("table", true, "Location where the processed email documents will be " +
                "stored. Eg., /user/foo/my_workspace/store/email.json");
    }

    public static void main(String[] args) throws IOException {
        CommandLineParser cliParser = new DefaultParser();
        try {
            CommandLine commandLine = cliParser.parse(OPTIONS, args);
            setupLogFile();
            setupConsoleLog();
            Config config = getConfig();
            String dataPath = getDataFolder(commandLine, config);
            String emailTablePath = getEmailStorePath(commandLine, dataPath);

            Injector injector = Guice.createInjector(new CoreModule(config, dataPath,
                            emailTablePath), new ParserModule());
            SQLContext sqlContext = injector.getInstance(SQLContext.class);
            DatasetUtil datasetUtil = injector.getInstance(DatasetUtil.class);
            ReportFactory reportFactory = injector.getInstance(ReportFactory.class);
            log("Loading email corpus");
            DataLoader dataLoader = injector.getInstance(DataLoader.class);
            dataLoader.load(dataPath, emailTablePath);
            log("Creating base dataset");
            Dataset<Row> baseDataset = loadBaseDataset(datasetUtil, emailTablePath, sqlContext);
            log("Creating exploded dataset");
            Dataset<Row> exploded = explodedDataset(datasetUtil, baseDataset);
            log("Loaded email corpus");

            printTableHelp(baseDataset, exploded);
            printReportHelp();
            System.out.print("analyzer>");
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String command = scanner.nextLine();
                try {
                    if (StringUtils.isNotBlank(command)) {
                        if (!checkExec(sqlContext, command)) {
                            if (!checkReport(reportFactory, baseDataset, exploded,
                                    command)) {
                                if (checkHelp(command)) {
                                    printTableHelp(baseDataset, exploded);
                                    printReportHelp();
                                }  else {
                                    System.err.println("Unknown command:" + command);
                                    printTableHelp(baseDataset, exploded);
                                    printReportHelp();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
                System.out.print("analyzer>");
            }

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            printHelp();
        }
    }

    private static void printTableHelp(Dataset<Row> baseDataset, Dataset<Row> exploded) {
        log("Tables available:\n");
        CONSOLE_LOG.info("Table name: emails\nSchema:\n{}\n", baseDataset.schema().sql());
        CONSOLE_LOG.info("Table name: exploded\nSchema:\n{}\n", exploded
                .schema().sql());
    }

    private static void printReportHelp() {
        log("To view prebuilt reports: type report <code>\n");
        log("Report codes:");
        Arrays.stream(PrebuildReportType.values())
                .forEach(report ->
                CONSOLE_LOG.info("{} : {}", report.getCode(), report.getMessage()));
        log("For reports, type command 'report <report_no>'");
        log("For free form query, type command 'exec <sql statement>'");
    }

    private static Dataset<Row> loadBaseDataset(DatasetUtil datasetUtil, String emailTablePath,
            SQLContext sqlContext) {
        Dataset<Row> dataset = sqlContext.read()
                .schema((StructType) StructType.fromJson(datasetUtil.getDefaultSchema()))
                .json(emailTablePath)
                .persist(StorageLevel.MEMORY_AND_DISK_SER());
        dataset = datasetUtil.explodeDateTime(dataset);
        dataset.createOrReplaceTempView("emails");
        return dataset;
    }

    private static Dataset<Row> explodedDataset(DatasetUtil datasetUtil,
            Dataset<Row> baseDataset) {
        Dataset<Row> dataFrame = datasetUtil.explode(baseDataset);
        dataFrame.createOrReplaceTempView("exploded");
        return dataFrame;
    }

    private static Config getConfig() {
        String confFile = Paths.get(".", "conf", "application.conf").toAbsolutePath()
                .normalize().toString();
        LOGGER.info("Loading configuration from {}", confFile);
        return ConfigFactory.load(confFile)
                .withFallback(ConfigFactory.load());
    }

    private static String getDataFolder(final CommandLine commandLine, final Config config) {
        String dataPath;
        if (config.hasPath(DATA)) {
            dataPath = config.getString(DATA);
        } else {
            dataPath = Paths.get(PATH_DOT, DATA).toAbsolutePath().normalize().toString();
        }
        dataPath = commandLine.getOptionValue(DATA, dataPath);
        LOGGER.info("Configured data folder as {}", dataPath);
        CONSOLE_LOG.info("Configured data folder as {}", dataPath);
        return dataPath;
    }


    private static String getEmailStorePath(final CommandLine commandLine, final String dataPath) {
        String emailTablePath = Paths.get(dataPath, String.format(EMAIL_JSON, TABLE_EMAILS))
                .toAbsolutePath().normalize().toString();
        emailTablePath = commandLine.getOptionValue("table", emailTablePath);

        try {
            Path path = Paths.get(emailTablePath);
            if (!path.toFile().exists()) {
                LOGGER.info("Creating email data file at {}", emailTablePath.toString());
                Files.createFile(path);
            }
        } catch (IOException e) {
            throw new AnalyzerException(e, "Error creating email data file at %s",
                    emailTablePath.toString());
        }
        LOGGER.info("Configured email store path as {}", emailTablePath);
        CONSOLE_LOG.info("Configured email store path as {}", emailTablePath);
        return emailTablePath;
    }

    private static boolean checkExec(SQLContext sqlContext, String command) {
        Matcher matcher = PATTERN_EXEC.matcher(command.trim());
        if (matcher.find()) {
            String commandToExecute = matcher.group(2);
            Dataset<Row> result = sqlContext.sql(commandToExecute);
            print(result);
            return true;
        }
        return false;
    }

    private static boolean checkReport(ReportFactory reportFactory,
            Dataset<Row> baseDataset, Dataset<Row> messageTypeDataset, String command) {
        Matcher matcher = PATTERN_REPORT.matcher(command.trim());
        if (matcher.find()) {
            String reportNumberStr = matcher.group(2);
            int reportNumber = Integer.parseInt(reportNumberStr);
            PrebuildReportType reportCode = PrebuildReportType.getReportEnumBy(reportNumber);
            if (reportCode != null) {
                Dataset<Row> report = reportFactory.generate(reportCode, baseDataset,
                        messageTypeDataset);
                print(report);
            } else {
                System.err.printf("Unknown report code: %s%n", reportNumberStr);
            }
            return true;
        }
        return false;
    }

    private static boolean checkHelp(final String command) {
        Matcher matcher = PATTERN_HELP.matcher(command.trim());
        return matcher.find();
    }

    private static void print(final Dataset<Row> result) {
        result.show(250);
    }

    private static void setupConsoleLog() {
        ConsoleAppender console = new ConsoleAppender(); //create appender
        //configure the appender
        String PATTERN = "%d{HH:mm:ss} %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.INFO);
        console.activateOptions();
        //add appender to any Logger (here is root)
        org.apache.log4j.Logger.getLogger("Console").addAppender(console);
    }

    private static void setupLogFile() throws IOException {
        RollingFileAppender fileAppender = new RollingFileAppender();
        fileAppender.setName("FileLogger");
        Path logDir = Paths.get(".", "log");
        if (!logDir.toFile().exists()) {
            System.out.printf("Creating new log directory: %s%n", logDir.toString());
            logDir.toFile().mkdirs();
        }
        Path logFilePath = Paths.get(".", "log", "analyzer.log");
        if (!logFilePath.toFile().exists()) {
            System.out.printf("Creating new log file: %s%n", logFilePath.toString());
            Files.createFile(logFilePath);
        }
        fileAppender.setFile(logFilePath.toFile().getAbsolutePath());
        fileAppender.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        fileAppender.setThreshold(Level.DEBUG);
        fileAppender.setAppend(true);
        fileAppender.setMaxFileSize("10MB");
        fileAppender.setMaxBackupIndex(5);
        fileAppender.activateOptions();
        org.apache.log4j.Logger.getRootLogger().addAppender(fileAppender);
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "java -jar analyzer.jar ", OPTIONS);
    }

    private static void log(String msg, Object... args) {
        CONSOLE_LOG.info(msg, args);
        LOGGER.info(msg, args);
    }
}
