package generateindices;

import Medline.*;
import java.io.*;
import java.util.*;
import javax.xml.bind.*;
import java.util.zip.*;
import java.util.concurrent.*;
import java.util.*;
import java.io.FileWriter;
import Misc.Config;
import Misc.FilePair;
import org.apache.log4j.*;

public class ProcessMedline implements Callable<String> {

    static Logger logger = Logger.getLogger(ProcessMedline.class);
    private static ConcurrentMap<String, Integer> conPmid =
            new ConcurrentHashMap<String, Integer>(1000);
//    private static Set<String> conPmid =
//            new ConcurrentSkipListSet<String>();

    //private static Set conPmid = Collections.synchronizedSet(new HashSet());
    //private static Set duplicates = Collections.synchronizedSet(new HashSet());
    
    private static ConcurrentMap<String, Integer> duplicates =
            new ConcurrentHashMap<String, Integer>(1000);
//    private static Set<String> duplicates =
//            new ConcurrentSkipListSet<String>();
    //private Map<String, Integer> tmpMap =
    //       new HashMap<String, Integer>(1000);
    List<Trec> trecList = new ArrayList<Trec>();
    Config configOptions;
    String medline_index;
    FilePair pair;

    public ProcessMedline() {
        
    }

    public ProcessMedline(FilePair pair, String medline_index, Config configOptions) {
        this.pair = pair;
        this.configOptions = configOptions;
        this.medline_index = new String(medline_index);
    }

    public void printData(List<Trec> trecList, FilePair pair) {
        Integer success = 0;
        FileWriter trec_out = null;
        String annot_trec_filename = pair.annot_trec_output_path;
        FileWriter annot_trec_out = null;

        String trec_filename = pair.trec_output_path;

        logger.debug("Processing: " + trec_filename);
        if (pair.write) {
            logger.debug("Writing: " + trec_filename);
            logger.debug("Writing: " + annot_trec_filename);
        }
        File trec_outFile = new File(trec_filename);
        File annot_trec_outFile = new File(annot_trec_filename);

        try {
            if (pair.write) {
                trec_out = new FileWriter(trec_outFile);
                annot_trec_out = new FileWriter(annot_trec_outFile);
                String header = "<?xml version=\"1.0\"?>\n\n<ROOT>\n";
                trec_out.write(header);
                annot_trec_out.write(header);
            }
            for (Trec record : trecList) {
//                synchronized (conPmid) {
//                    success = conPmid.add(record.getDocno());
//                }
                try {
                    success = conPmid.putIfAbsent(record.getDocno(), 1);
                } catch (NullPointerException ex) {
                    logger.error(ex);
                }


                if (success == null) {
                    if (pair.write) {
                        trec_out.write(record.getXmlString());
                        annot_trec_out.write(record.getAnnotatorXmlString());
                    }
                } else {
//                    synchronized (duplicates) {
//                        success = duplicates.add(record.getDocno());
//                    }
                    duplicates.putIfAbsent(record.getDocno(), 1);
                }
            }
            logger.debug("HashMap size: " + conPmid.size());
            logger.debug("Duplicate size: " + duplicates.size());
            if (pair.write) {
                trec_out.write("\n</ROOT>\n");
                annot_trec_out.write("\n</ROOT>\n");
                trec_out.close();
                annot_trec_out.close();
            }
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public String call() throws Exception {

        int run_curr = 0;
        InputStream theFile = new FileInputStream(pair.input_path);
        Trec currTrec;
        List<MedlineCitation> listOfCitations;
        MedlineCitationSet citationset;

        JAXBContext context = JAXBContext.newInstance("Medline");
        Unmarshaller unmarshaller = context.createUnmarshaller();

        logger.debug("Reading Medline gz file: " + pair.input_path);
        try {
             citationset = (MedlineCitationSet) unmarshaller.unmarshal(
                    new GZIPInputStream(theFile));
        } catch (Exception ex) {
            logger.error(ex);
            return "Error: " + pair.input_path;
        }
        listOfCitations = citationset.getMedlineCitation();
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
