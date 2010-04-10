package generateindices;

import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import Misc.Config;
import Misc.FilePair;
import java.io.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.*;

public class BasicThread {

    static Logger logger = Logger.getLogger(BasicThread.class);

    public static void runManyExecutor(Config configOptions) {
        logger.debug("Threads: " + configOptions.num_threads);

        ExecutorService exec;

        //if (configOptions.threading) {
        exec = Executors.newFixedThreadPool(configOptions.num_threads);
        //}

        ProcessMedline staticProcessMedline;
        List<String> files = new ArrayList<String>();
        List<FilePair> filePair = new ArrayList<FilePair>();
        FilePair currPair;
        boolean do_write = false;

        if ((configOptions.generate_baseline) && (configOptions.generate_update)) {
            configOptions.min_run = configOptions.baseline_start;
            configOptions.max_run = configOptions.update_end;
        } else if (configOptions.generate_baseline) {
            configOptions.min_run = configOptions.baseline_start;
            configOptions.max_run = configOptions.baseline_end;
        } else if (configOptions.generate_update) {
            configOptions.min_run = configOptions.update_start;
            configOptions.max_run = configOptions.update_end;
        }

        configOptions.generateTrecIndriSearchParamFile();
        configOptions.generateTrecIndriTextMiningAnnotParamFile();

        String medline_prefix = FilenameUtils.concat(configOptions.source_dir, configOptions.medline_file_base);
        String medline_index = new String();

        ArrayList<Future<String>> results = new ArrayList<Future<String>>();

        for (int i = configOptions.baseline_start; i <= configOptions.max_run; i++) {
            do_write = true;
            if (i < configOptions.min_run) {
                do_write = false;
            }
            medline_index = new String(String.format("%04d", i));
            String input_path = new String(medline_prefix
                    + medline_index
                    + configOptions.input_postfix);
            String common_postfix = new String(configOptions.medline_file_base
                    + medline_index
                    + configOptions.trec_postfix);
            String trec_output_path = FilenameUtils.concat(configOptions.IndriSearch_source_dir,
                    common_postfix);
            String annot_trec_output_path = FilenameUtils.concat(configOptions.IndriTextMining_source_dir,
                    common_postfix);
            currPair = new FilePair(i, do_write, input_path, trec_output_path, annot_trec_output_path);
            filePair.add(currPair);
        }
        for (FilePair pair : filePair) {
            if (configOptions.threading) {
                results.add(exec.submit(new ProcessMedline(pair, medline_index, configOptions)));
            } else {
                staticProcessMedline = new ProcessMedline(pair, medline_index, configOptions);
                try {
                    staticProcessMedline.call();
                } catch (Exception ex) {
                    logger.error(ex);
                }
            }
        }
        if (configOptions.threading) {
            for (Future<String> fs : results) {
                try {
                    logger.debug(fs.get());
                } catch (InterruptedException e) {
                    logger.error(e);
                    return;
                } catch (ExecutionException e) {
                    logger.error(e);
                } finally {
                    exec.shutdown();
                }
            }
        }
    }
}
