package Misc;

public class FilePair {

    public String input_path;
    public String trec_output_path;
    public String annot_trec_output_path;
    public Integer index;
    public boolean write;

    public FilePair(Integer index, boolean write, String input_path, String trec_output_path, String annot_trec_output_path) {
        this.index = index;
        this.write = write;
        this.input_path = input_path;
        this.trec_output_path = trec_output_path;
        this.annot_trec_output_path = annot_trec_output_path;
    }
}
