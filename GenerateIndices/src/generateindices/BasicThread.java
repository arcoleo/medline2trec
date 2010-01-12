package generateindices;

import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import Misc.Config;
import Misc.FilePair;
import java.io.*;

public class BasicThread {

    public static void runManyExecutor(Config configOptions) {
        ExecutorService exec = Executors.newFixedThreadPool(configOptions.num_threads);
        List<String> files = new ArrayList<String>();
        List<FilePair> filePair = new ArrayList<FilePair>();
        FilePair currPair;

        configOptions.generateTrecIndriParamFile();

        String medline_prefix = configOptions.source_dir + configOptions.medline_file_base;
        String medline_index = new String();

        ArrayList<Future<String>> results =
                new ArrayList<Future<String>>();
        for (int i = configOptions.start_file; i <= configOptions.end_file; i++) {
            medline_index = new String(String.format("%04d", i));
            String input_path = new String(medline_prefix
                    + medline_index
                    + configOptions.input_postfix);
            String common_postfix = new String(configOptions.medline_file_base +
                    medline_index +
                    configOptions.trec_postfix);
            String trec_output_path = new String(configOptions.trec_dir +
                    common_postfix);
            String annot_trec_output_path = new String(configOptions.annot_dir +
                    common_postfix);
            currPair = new FilePair(i, input_path, trec_output_path, annot_trec_output_path);
            filePair.add(currPair);
        }
        for (FilePair pair : filePair) {
            System.out.println("Running with: " + pair.input_path);
            results.add(exec.submit(new ProcessMedline(pair, medline_index, configOptions)));
        }
        for (Future<String> fs : results) {
            try {
                System.out.println(fs.get());
            } catch (InterruptedException e) {
                System.out.println(e);
                return;
            } catch (ExecutionException e) {
                System.out.println(e);
            } finally {
                exec.shutdown();
            }
        }
    }
}
