package Misc;

import java.io.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.*;

public class Config implements Cloneable {

    static Logger logger = Logger.getLogger(Config.class);

    // dir parent of source repo
    public String base_dir;
    // traditionally non_repo_data dir
    public String data_dir;
    // traditionally non_repo_data/YYYYMM
    public String future_dir;
    public String baseline_dir;

    /* dir with baseline (reset every year) data
     * traditionally parent_target_dir/gz_baseline_ascii
     */
    public String source_dir;
    public String IndriSearch_source_dir;
    public String IndriTextMining_source_dir;
    public String IndriSearch_dir;
    public String IndriTextMiningAnnotSearch_dir;
    public boolean verbose;
    public boolean threading;
    public boolean generate_baseline;
    public boolean generate_update;
    public String trecIndriSearchParamFile;
    public String trecIndriTextMiningParamFile;
    // to be translated to medline10n + startfile.xml.gz
    public Integer baseline_start;
    public Integer baseline_end;
    public Integer update_start;
    public Integer update_end;
    public Integer run_short;
    public Integer min_run;
    public Integer max_run;
    public String medline_file_base;
    public Integer num_threads;
    public String input_postfix;
    public String trec_postfix;

    public Config() {
        String home_dir = System.getProperty("user.home");
        this.base_dir = FilenameUtils.concat(home_dir, "repo/hg-igb");
        this.data_dir = FilenameUtils.concat(this.base_dir, "non_repo_data");
        this.future_dir = FilenameUtils.concat(this.data_dir, "future");
        this.baseline_dir = FilenameUtils.concat(this.data_dir, "baseline");

        this.IndriSearch_source_dir = FilenameUtils.concat(this.future_dir, "Medline/trec");
        this.IndriTextMining_source_dir = FilenameUtils.concat(this.future_dir, "Medline/annot");
        this.IndriSearch_dir = FilenameUtils.concat(this.future_dir, "Medline/IndriSearch");
        this.IndriTextMiningAnnotSearch_dir = FilenameUtils.concat(this.future_dir, "Medline/IndriTextMining_annotator");
        this.trecIndriSearchParamFile = FilenameUtils.concat(this.future_dir, "Medline/IndriSearchParam.xml");
        this.trecIndriTextMiningParamFile = FilenameUtils.concat(this.future_dir, "Medline/IndriTextMiningParam.xml");

        // TODO: set this default to false
        this.generate_baseline = false;
        this.generate_update = false;

        // NOTE: use 413 for start and end to debug utf8
        this.baseline_start = 1;
        this.baseline_end = 617; //656 or 617;
        this.update_start = this.baseline_end + 1;
        this.update_end = 1000;
        this.min_run = 1;
        this.max_run = 1;

        this.medline_file_base = "medline10n";
        this.num_threads = 4;
        this.input_postfix = ".xml.gz";
        this.trec_postfix = ".trec";
        this.run_short = 30000;
    }

    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ex) {
            logger.error(ex);
            return null;
        }
    }

    public void generateTrecIndriSearchParamFile() {
        logger.debug("Writing to :" + trecIndriSearchParamFile);
        String param = "<parameters>\n"
                + "<memory>8G</memory>\n"
                + "<index>" + IndriSearch_dir + "</index>\n"
                + "<corpus>\n"
                + "    <class>trectext</class>\n"
                + "    <path>" + IndriSearch_source_dir + "</path>\n"
                + "</corpus>\n"
                + "<field>\n    <name>docno</name>\n</field>\n"
                + "<field>\n    <name>pmid</name>\n</field>\n"
                + "<field>\n    <name>issn</name>\n</field>\n"
                + "<field>\n    <name>pubdate</name>\n</field>\n"
                + "<field>\n    <name>source</name>\n</field>\n"
                + "<field>\n    <name>title</name>\n</field>\n"
                + "<field>\n    <name>abstract</name>\n</field>\n"
                + "<field>\n    <name>author</name>\n</field>\n"
                + "<field>\n    <name>chemicals</name>\n</field>\n"
                + "<field>\n    <name>genes</name>\n</field>\n"
                + "<field>\n    <name>mesh</name>\n</field>\n"
                + "<field>\n    <name>keywords</name>\n</field>\n"
                + "</parameters>\n";

        File trec_indri_outFile = new File(trecIndriSearchParamFile);
        try {
            FileWriter trec_indri_out = new FileWriter(trec_indri_outFile);
            trec_indri_out.write(param);
            trec_indri_out.close();
        } catch (IOException ex) {
            logger.error(ex);
        }
    }

    public void generateTrecIndriTextMiningAnnotParamFile() {
        // TODO: repo/hg-igb/source/BeeSpace4/Index/2010/annotator-list-future-YYYYMMDD
        // it should look like /path/to/future/Medline/annot/medline--.trec
        StringBuffer annotFileList = new StringBuffer();
        String formatted;
        String trec_file;
        for (Integer i = min_run; i <= max_run; i++) {
            formatted = String.format("%04d", i);
            trec_file = "medline10n" + formatted + ".trec";
            annotFileList.append(FilenameUtils.concat(IndriTextMining_source_dir, trec_file));
        }

        Date dateNow = new Date();
        SimpleDateFormat dateformatYYYYMMDD = new SimpleDateFormat("yyyyMMdd");
        StringBuilder newYYYYMMDD = new StringBuilder(dateformatYYYYMMDD.format(dateNow));
        
        // this.base_dir + 'source' + BeeSpace4 + Index + 2010 + annotator-list-YYYYMMDD
        String annot_list_file_str = FilenameUtils.concat(this.base_dir, "source/BeeSpace4/Index/2010/annotator-list-" + newYYYYMMDD);
        File annot_list_file = new File(annot_list_file_str);
        logger.debug("Writing to: " + annot_list_file);
        try {
            FileWriter annot_list_file_writer = new FileWriter(annot_list_file);
            annot_list_file_writer.write(annotFileList.toString());
            annot_list_file_writer.close();
        } catch (IOException ex) {
            logger.error(ex);
        }

        
        String param = "<parameters>\n"
                + "<memory>8G</memory>\n"
                + "<index>" + IndriTextMiningAnnotSearch_dir + "</index>\n"
                + "<corpus>\n"
                + "    <class>trectext</class>\n"
                + "    <path>" + IndriTextMining_source_dir + "</path>\n"
                + "</corpus>\n"
                + "<field>\n    <name>text</name>\n</field>\n"
                + "<stemmer><name>krovetz</name></stemmer>\n"
                + "</parameters>\n";

        logger.debug("Writing to: " + trecIndriSearchParamFile);
        File trec_indri_outFile = new File(trecIndriSearchParamFile);
        try {
            FileWriter trec_indri_out = new FileWriter(trec_indri_outFile);
            trec_indri_out.write(param);
            trec_indri_out.close();
        } catch (IOException ex) {
            logger.error(ex);
        }
    }
}
