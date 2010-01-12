package Misc;

import java.io.*;

public class Config  implements Cloneable {

    public String base_dir;
    public String data_dir;
    public String date_dir;
    public String source_dir;
    public String trec_dir;
    public String annot_dir;
    public boolean verbose;
    public String trec_indri_param_file;
    // to be translated to medline10n + startfile.xml.gz
    public Integer start_file;
    public Integer end_file;
    public String medline_file_base;
    public Integer num_threads;
    public String input_postfix;
    public String trec_postfix;
    public Integer run_short;

    public Config() {
        String os = System.getProperty("os.name");
        String os_prefix = "/home";
        if (os.compareTo("Mac OS X") == 0) {
            os_prefix = "/Users";
        }
        this.base_dir = os_prefix + "/arcoleo/repo/hg-igb/";
        this.data_dir = this.base_dir + "non_repo_data/";
        this.date_dir = this.data_dir + "2010/Medline/";
        this.source_dir = this.date_dir + "gz/";
        this.trec_dir = this.date_dir + "trec/";
        this.annot_dir = this.date_dir + "annot/";
        this.trec_indri_param_file = this.date_dir + "indri-param-all.xml";

        // FIXME: use 413 for start and end to debug utf8
        this.start_file = 413;
        this.end_file = 413; //656;
        this.medline_file_base = "medline10n";
        this.num_threads = 4;
        this.input_postfix = ".xml.gz";
        this.trec_postfix = ".trec";
        this.run_short = 30000;
    }

    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            System.out.println("Clone Ex: " + e);
            return null;
        }
    }

    public void generateTrecIndriParamFile() {
        String param = "<parameters>\n"
                + "<memory>8G</memory>\n"
                + "<index>non_repo_data/Medline/2010/indri</index>\n"
                + "<corpus>\n"
                + "    <class>trectext</class>\n"
                + "    <path>" + trec_dir + "</path>\n"
                + "</corpus>\n"
                + "<field>\n    <name>docno</name>\n"
                + "<field>\n    <name>pmid</name>\n"
                + "<field>\n    <name>issn</name>\n"
                + "<field>\n    <name>pubdate</name>\n"
                + "<field>\n    <name>source</name>\n"
                + "<field>\n    <name>title</name>\n"
                + "<field>\n    <name>abstract</name>\n"
                + "<field>\n    <name>author</name>\n"
                + "<field>\n    <name>chemicals</name>\n"
                + "<field>\n    <name>genes</name>\n"
                + "<field>\n    <name>mesh</name>\n"
                + "<field>\n    <name>keywords</name>\n"
                + "</parameters>\n";

        File trec_indri_outFile = new File(trec_indri_param_file);
        try {
            FileWriter trec_indri_out = new FileWriter(trec_indri_outFile);
            trec_indri_out.write(param);
            trec_indri_out.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
