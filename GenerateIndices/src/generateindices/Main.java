package generateindices;

import java.io.*;
import java.util.Map;
import org.apache.log4j.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import Misc.*;

public class Main {

    public static Config configOptions = new Config();
    static Logger logger = Logger.getLogger(Main.class);

    private static void setupLogger() throws IOException {
        //PatternLayout layout = new PatternLayout("%d{ISO8601} %p %c %x - %m%n");
        String logFile = "generate_indices.log";
        PatternLayout layout = new PatternLayout("%5p [%c:%L] %x - %m%n");
        ConsoleAppender appender = new ConsoleAppender(layout);
        FileAppender fappender = new FileAppender(layout, logFile);
        fappender.setImmediateFlush(true);
        BasicConfigurator.configure(appender);
        BasicConfigurator.configure(fappender);
    }

    public static void parse_options(Options options, String[] args) {
        CommandLine cl = null;
        CommandLineParser parser = new PosixParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            cl = parser.parse(options, args);
        } catch (ParseException ex) {
            System.out.println("Exception: " + ex.getMessage());
        }

        if (cl.hasOption("help")) {
            formatter.printHelp("GenerateIndices", options);
            System.out.println();
            System.exit(0);
        }

        // Get path options
        if (cl.hasOption("base_dir")) {
            configOptions.base_dir = new String(cl.getOptionValue("base_dir"));
        }
        if (cl.hasOption("baseline_dir")) {
            configOptions.baseline_dir = new String(cl.getOptionValue("baseline_dir"));
        }
        if (cl.hasOption("future_dir")) {
            configOptions.future_dir = new String(cl.getOptionValue("future_dir"));
        }

        // Get generation options
        if (cl.hasOption("generate_baseline")) {
            configOptions.generate_baseline = true;
        }
        if (cl.hasOption("generate_update")) {
            configOptions.generate_update = true;
        }

        // Get processing limiting options
        if (cl.hasOption("baseline_start")) {
            configOptions.baseline_start = new Integer(cl.getOptionValue("baseline_start"));
        }
        if (cl.hasOption("baseline_end")) {
            configOptions.baseline_end = new Integer(cl.getOptionValue("baseline_end"));
        }
        if (cl.hasOption("update_start")) {
            configOptions.update_start = new Integer(cl.getOptionValue("update_start"));
        }
        if (cl.hasOption("update_end")) {
            configOptions.update_end = new Integer(cl.getOptionValue("update_end"));
        }

        if (cl.hasOption("threading")) {
            configOptions.threading = true;
        }

        if (cl.hasOption("threads")) {
            configOptions.num_threads = new Integer(cl.getOptionValue("threads"));
        }
        if (cl.hasOption("short")) {
            configOptions.run_short = new Integer(cl.getOptionValue("short"));
        }

        if (cl.hasOption("cores")) {
            configOptions.num_threads = new Integer(cl.getOptionValue("cores"));
        }

        if (cl.hasOption("log_file")) {
        }

        if (cl.hasOption("verbose")) {
            logger.debug("base_dir: " + configOptions.base_dir);
            logger.debug("data_dir: " + configOptions.data_dir);
            logger.debug("start_file: " + configOptions.baseline_start);
            logger.debug("end_file: " + configOptions.baseline_end);
            logger.debug("threads: " + configOptions.num_threads);
        }
    }

    public static void init_options(String[] args) {
        Options options = new Options();

        options.addOption("a", "all", true, "Foobar");
        options.addOption("b", "base_dir", true,
                "dir which is parent of source, non_repo_data, etc");
        options.addOption("baseline_dir", true,
                "baseline source dir.  Typically .../non_repo_data/baseline");
        options.addOption("future_dir", true,
                "future dir.  Peer of baseline, typically .../non_repo_data/future");

        options.addOption("generate_baseline", false,
                "Generate baseline files.  Only use this if reprocessing *all* data");
        options.addOption("generate_update", false,
                "Generate update files.  Do this mostly.");

        options.addOption("baseline_start", true, "Start baseline file to process");
        options.addOption("baseline_end", true, "End baseline file to process");
        options.addOption("update_start", true, "Start update file to process");
        options.addOption("update_end", true, "End update file to process");
        options.addOption("short", true, "Only process subset of each trec file");

        options.addOption("r", "trec", false,
                "generate trec files.  This is necessary for indri and entity steps");
        options.addOption("future_dir", true,
                "Dir which will be parent of generated output dirs");
        options.addOption("i", "indri", false, "build indri");
        options.addOption("t", "entity", false, "build entity");
        options.addOption("small_trec", false,
                "write out trec files with no more than 1k elements");
        options.addOption("cores", true, "number of cores to use");

        options.addOption("threads", true, "Test threads");
        options.addOption("threading", false, "Use threading");
        options.addOption("v", "verbose", false, "verbose output");
        options.addOption("h", "help", false, "help");
        options.addOption("log_file", true, "Log file");

        parse_options(options, args);
    }

    public static void init_dirs() {
        // setup paths
        File f;
        String future_dir;
        boolean success;
        String[] dirs = {configOptions.base_dir, configOptions.baseline_dir, configOptions.future_dir};
        String[] future_dirs = {"Medline/trec", "Medline/annot", "Medline/gz_links"};

        for (String curr_dir : dirs) {
            f = new File(curr_dir);
            if (!f.isDirectory()) {
                logger.error("Dir does not exist.  Quitting.: " + curr_dir);
                System.exit(1);
            }
        }

        for (String curr_dir : future_dirs) {
            future_dir = FilenameUtils.concat(configOptions.future_dir, curr_dir);
            f = new File(future_dir);
            if (!f.exists()) {
                try {
                    if (!f.mkdirs()) {
                        logger.error("Cannot create dir [" + future_dir + "]");
                        System.exit(1);
                    }

                } catch (SecurityException e) {
                    logger.error("Cannot create dir [" + future_dir + "]: " + e.getMessage());
                    System.exit(1);
                }
            }
        }
    }

    /* Create future/Medline/gz_links which contains hardlinks to all necessary
     * files.
     */
    public static void init_links() {
        String links_dir = FilenameUtils.concat(configOptions.future_dir, "Medline/gz_links");
        String make_links = FilenameUtils.concat(links_dir, "make_links");
        StringBuilder script = new StringBuilder();
        int l1 = 0;
        int l2 = 0;

        script.append("#!/bin/bash\n\n");


        // ln ../../baseline/Medline/gz_baseline_ascii/medline10n0001.xml.gz .
        // ln baseline_dir/medline10n0001.xml.gz .

        configOptions.source_dir = FilenameUtils.concat(configOptions.baseline_dir, "Medline/gz_baseline_ascii");
        File baselineDir = new File(configOptions.source_dir);
        File[] baselineFiles = baselineDir.listFiles(new XMLFileFilter());

        logger.debug("future_dir: " + configOptions.future_dir);
        logger.debug("baseline_dir: " + configOptions.baseline_dir);
        logger.debug("source_dir: " + configOptions.source_dir);


        l1 = baselineFiles.length;

        for (File f : baselineFiles) {
            script.append("ln " + f.toString() + " . \n");
        }

        script.append("\n\n");

        if (configOptions.generate_update) {
            configOptions.source_dir = FilenameUtils.concat(configOptions.future_dir, "Medline/gz_update_ascii");
            File futureDir = new File(configOptions.source_dir);
            File[] futureFiles = futureDir.listFiles(new XMLFileFilter());

            logger.debug("futureFiles size: " + futureFiles);

            for (File f : futureFiles) {
                script.append("ln " + f.toString() + " . \n");
            }
            script.append("\n\n");

            l2 = futureFiles.length;
        }

        configOptions.source_dir = links_dir;
        logger.debug("source_dir: " + configOptions.source_dir);
        logger.debug("L1: " + l1 + ", L2: " + l2);

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(make_links));
            out.append(script.toString());
            out.close();
        } catch (IOException e) {
            logger.error(e);
        }

        ProcessBuilder pb = new ProcessBuilder("bash", "make_links");
        Map<String, String> env = pb.environment();
        pb.directory(new File(links_dir));
        try {
            Process p = pb.start();
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public static void main(String[] args) throws Exception {
        setupLogger();
        init_options(args);
        init_dirs();
        init_links();
        BasicThread.runManyExecutor(configOptions);
    }
}
