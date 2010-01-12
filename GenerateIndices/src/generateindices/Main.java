package generateindices;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
import Misc.*;

public class Main {

    public static Config configOptions = new Config();

    public static void init_options(String[] args) {
        CommandLine cl = null;
        CommandLineParser parser = new PosixParser();
        Options options = new Options();

        options.addOption("a", "all", true, "Foobar");
        options.addOption("b", "base_dir", true,
            "dir which is parent of source, non_repo_data, etc");
        options.addOption("r", "trec", false,
            "generate trec files.  This is necessary for indri and entity steps");
        options.addOption("i", "indri", false, "build indri");
        options.addOption("t", "entity", false, "build entity");
        options.addOption("small_trec", false,
            "write out trec files with no more than 1k elements");
        options.addOption("c", "cores", true, "number of cores to use");
        options.addOption("s", "start", true, "Start file to process");
        options.addOption("short", true, "Only process subset of each trec file");
        options.addOption("e", "end", true, "End file to process");
        options.addOption("threads", true, "Test threads");
        options.addOption("v", "verbose", false, "verbose output");
        options.addOption("h", "help", false, "help");

        try {
            cl = parser.parse(options, args);
        } catch (ParseException ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
        HelpFormatter formatter = new HelpFormatter();

        if (cl.hasOption("help")) {
            formatter.printHelp("GenerateIndices", options);
            System.out.println();
            System.exit(0);
        }
        if (cl.hasOption("base_dir")) {
            //configOptions.base_dir = new String()
            configOptions.base_dir = new String(cl.getOptionValue("b"));
        }
        if (cl.hasOption("start")) {
            configOptions.start_file = new Integer(cl.getOptionValue("s"));
        }
        if (cl.hasOption("end")) {
            configOptions.end_file = new Integer(cl.getOptionValue("e"));
        }
        if (cl.hasOption("threads")) {
            configOptions.num_threads = new Integer(cl.getOptionValue("threads"));
        }
        if (cl.hasOption("short")) {
            configOptions.run_short = new Integer(cl.getOptionValue("short"));
        }
        if (cl.hasOption("verbose")) {
            System.out.println("base_dir: " + configOptions.base_dir);
            System.out.println("data_dir: " + configOptions.data_dir);
            System.out.println("start_file: " + configOptions.start_file);
            System.out.println("end_file: " + configOptions.end_file);
            System.out.println("threads: " + configOptions.num_threads);
        }
    }

    public static void main(String[] args) {
        init_options(args);
        BasicThread.runManyExecutor(configOptions);
    }
}
