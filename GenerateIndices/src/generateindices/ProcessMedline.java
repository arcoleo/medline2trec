package generateindices;

import Medline.*;
import java.io.*;
import java.util.*;
import javax.xml.bind.*;
import java.util.zip.*;
import java.util.concurrent.*;
import java.io.FileWriter;
import Misc.Config;
import Misc.FilePair;

public class ProcessMedline implements Callable<String> {

    //private static Map<String, Integer> conPmid =
    //    new ConcurrentHashMap<String, Integer>(1000);
    List<Trec> trecList = new ArrayList<Trec>();
    Config configOptions;
    String medline_index;
    FilePair pair;

    public ProcessMedline(FilePair pair, String medline_index, Config configOptions) {
        this.pair = pair;
        this.configOptions = new Config();
        this.medline_index = new String(medline_index);
    }

    public void printData(List<Trec> trecList, FilePair pair) {
        String trec_filename = pair.trec_output_path;
        String annot_trec_filename = pair.annot_trec_output_path;
        System.out.println("Writing: " + trec_filename);
        System.out.println("Writing: " + annot_trec_filename);
        File trec_outFile = new File(trec_filename);
        File annot_trec_outFile = new File(annot_trec_filename);
        try {
            FileWriter trec_out = new FileWriter(trec_outFile);
            FileWriter annot_trec_out = new FileWriter(annot_trec_outFile);
            String header = "<?xml version=\"1.0\"?>\n\n<ROOT>\n";
            trec_out.write(header);
            annot_trec_out.write(header);
            for (Trec record : trecList) {
                trec_out.write(record.getXmlString());
                annot_trec_out.write(record.getAnnotatorXmlString());
            }
            trec_out.write("\n</ROOT>\n");
            annot_trec_out.write("\n</ROOT>\n");
            trec_out.close();
            annot_trec_out.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public String call() throws Exception {

        int run_curr = 0;
        InputStream theFile = new FileInputStream(pair.input_path);
        Trec currTrec;

        JAXBContext context = JAXBContext.newInstance("Medline");
        Unmarshaller unmarshaller = context.createUnmarshaller();

        MedlineCitationSet citationset = (MedlineCitationSet) unmarshaller.unmarshal(
            //new FileReader(path));
            new GZIPInputStream(theFile));
        List<MedlineCitation> listOfCitations = citationset.getMedlineCitation();
        for (MedlineCitation currCitation : listOfCitations) {
            run_curr++;
            currTrec = new Trec(currCitation);
            trecList.add(currTrec);
            if (run_curr >= configOptions.run_short) {
                break;
            }
        }
        theFile.close();
        printData(trecList, pair);
        trecList = null;
        listOfCitations = null;
        citationset = null;
        unmarshaller = null;
        context = null;
        return "Done";
    }
}
