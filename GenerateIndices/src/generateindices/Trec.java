package generateindices;

import Medline.*;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.UnmappableCharacterException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

public class Trec {

    private String Docno;
    private String Pmid;
    private String Source;
    private String Title;
    private String Abstract;
    private String Issn;
    private List<String> Chemicals;
    private List<String> Mesh;
    private String Pubdate;
    private List<String> Authors;
    private String[] Genes;
    private String[] Keywords;

    public Trec(MedlineCitation currCitation) {
        this.setDocno(currCitation);
        this.setPmid(currCitation);
        this.setTitle(currCitation);
        this.setAbstract(currCitation);
        this.setIssn(currCitation);
        this.setPubdate(currCitation);
        this.setSource(currCitation);
        this.setAuthors(currCitation);
        this.setChemicals(currCitation);
        this.setMesh(currCitation);
    }

    /**
     * @return the Docno
     */
    public String getDocno() {
        return Docno;
    }

    /**
     * @param Docno the Docno to set
     */
    public void setDocno(String Docno) {
        this.Docno = Docno;
    }

    /**
     * @param Docno the Docno to set
     */
    public void setDocno(MedlineCitation currCitation) {
        try {
            this.Docno = "pmid-" + currCitation.getPMID().getvalue();
        } catch (Exception ex) {
            System.out.println("Ex:setDocno: " + ex);
            this.Docno = "--";
        }
    }

    /**
     * @return the Pmid
     */
    public String getPmid() {
        return Pmid;
    }

    /**
     * @param Pmid the Pmid to set
     */
    public void setPmid(String Pmid) {
        this.Pmid = Pmid;
    }

    /**
     * @param Pmid the Pmid to set
     */
    public void setPmid(MedlineCitation currCitation) {
        try {
            this.Pmid = currCitation.getPMID().getvalue();
        } catch (Exception ex) {
            System.out.println("Ex:setPmid: " + ex);
            this.Pmid = "--";
        }
    }

    /**
     * @return the Source
     */
    public String getSource() {
        return Source;
    }

    /**
     * @param Source the Source to set
     */
    public void setSource(String Source) {
        this.Source = Source;
    }

    public void setSource(MedlineCitation currCitation) {
        try {
            this.Source = currCitation.getArticle().getJournal().getTitle();
        } catch (Exception ex) {
            System.out.println("Ex:setSource: " + ex);
            this.Source = "--";
        }
    }

    /**
     * @return the Title
     */
    public String getTitle() {
        return Title;
    }

    /**
     * @param Title the Title to set
     */
    public void setTitle(String Title) {
        this.Title = Title;
    }

    /**
     * @param Title the Title to set
     */
    public void setTitle(MedlineCitation currCitation) {
        try {
            this.Title = currCitation.getArticle().getArticleTitle();
        } catch (Exception ex) {
            System.out.println("Ex:setTitle: " + ex);
            this.Title = "--";
        }
    }

    /**
     * @return the Abstract
     */
    public String getAbstract() {
        return Abstract;
    }

    /**
     * @param Abstract the Abstract to set
     */
    public void setAbstract(String Abstract) {
        this.Abstract = Abstract;
    }

    /**
     * @param Abstract the Abstract to set
     */
    public void setAbstract(MedlineCitation currCitation) {
        try {
            this.Abstract = currCitation.getArticle().getAbstract().getAbstractText();
        } catch (Exception ex) {
            // FIXME: get other abtract
            this.Abstract = "";
        }
    }

    /**
     * @return the Issn
     */
    public String getIssn() {
        return Issn;
    }

    /**
     * @param Issn the Issn to set
     */
    public void setIssn(String Issn) {
        this.Issn = Issn;
    }

    public void setIssn(MedlineCitation currCitation) {
        try {
            this.Issn = currCitation.getArticle().getJournal().getISSN().getvalue();
        } catch (Exception ex) {
            //System.out.println("Ex:setIssn: " + ex);
            this.Issn = "--";
        }
    }

    /**
     * @return the Chemicals
     */
    public List<String> getChemicals() {
        return Chemicals;
    }

    /**
     * @param Chemicals the Chemicals to set
     */
    public void setChemicals(List<String> Chemicals) {
        this.Chemicals = Chemicals;
    }

    public void setChemicals(MedlineCitation currCitation) {
        ChemicalList chemicals = currCitation.getChemicalList();
        this.Chemicals = new ArrayList<String>();
        if (chemicals == null) {
            return;
        }
        for (Chemical currChemical : chemicals.getChemical()) {
            try {
                this.Chemicals.add(currChemical.getNameOfSubstance());
            } catch (Exception ex) {
                System.out.println("Ex:setChemicals: " + ex);
            }
        }
    }

    /**
     * @return the Mesh
     */
    public List<String> getMesh() {
        return Mesh;
    }

    /**
     * @param Mesh the Mesh to set
     */
    public void setMesh(List<String> Mesh) {
        this.Mesh = Mesh;
    }

    public void setMesh(MedlineCitation currCitation) {
        MeshHeadingList meshHeadings = currCitation.getMeshHeadingList();
        this.Mesh = new ArrayList<String>();
        if (meshHeadings == null) {
            return;
        }
        for (MeshHeading currMesh : meshHeadings.getMeshHeading()) {
            try {
                this.Mesh.add(currMesh.getDescriptorName().getvalue());
            } catch (Exception ex) {
                System.out.println("Ex:setMesh: " + ex);
            }
        }
    }

    /**
     * @return the Pubdate
     */
    public String getPubdate() {
        return Pubdate;
    }

    /**
     * @param Pubdate the Pubdate to set
     */
    public void setPubdate(String Pubdate) {
        this.Pubdate = Pubdate;
    }

    public void setPubdate(MedlineCitation currCitation) {
        // FIXME: get proper date
        try {
            this.Pubdate = currCitation.getArticle().getJournal().getJournalIssue().getPubDate().getYearOrMonthOrDayOrSeasonOrMedlineDate().toString();
        } catch (Exception ex) {
            System.out.println("Ex:setPubdate: " + ex);
            this.Pubdate = "--";
        }
    }

    /**
     * @return the Authors
     */
    public List<String> getAuthors() {
        return Authors;
    }

    /**
     * @param Authors the Authors to set
     */
    public void setAuthors(List<String> Authors) {
        this.Authors = Authors;
    }

    public void setAuthors(MedlineCitation currCitation) {
        String fname = "";
        String lname = "";
        this.Authors = new ArrayList<String>();
        AuthorList authors = currCitation.getArticle().getAuthorList();
        String padding = " : ";
        if (authors == null) {
            return;
        }
        for (Author currAuthor : authors.getAuthor()) {
            fname = "";
            lname = "";
            for (Object currObject : currAuthor.getLastNameOrForeNameOrInitialsOrSuffixOrNameIDOrCollectiveName()) {
                Object currObj2 = null;
                String val = "";
                Object currClass = currObject.getClass().cast(currObject);
                if (currObject instanceof Medline.LastName) {
                    currObj2 = (LastName) currObject;
                    lname = ((LastName) currObj2).getvalue();
                } else if (currObject instanceof Medline.ForeName) {
                    currObj2 = (ForeName) currObject;
                    fname = ((ForeName) currObj2).getvalue();
                } else if (currObject instanceof Medline.Initials) {
                    currObj2 = (Initials) currObject;
                    val = ((Initials) currObj2).getvalue();
                }
            }
            try {
                this.Authors.add(fname + " " + lname + padding);
            } catch (Exception ex) {
                System.out.println("Ex:setAuthors: " + ex);
            }
        }

        // remove separator
        this.Authors.remove(this.Authors.size() - 1);
        this.Authors.add(fname + " " + lname);
        //for (String curr : this.Authors) {
        //    System.out.println(curr);
        //}

    }

    /**
     * @return the genes
     */
    public String[] getGenes() {
        return Genes;
    }

    /**
     * @param genes the genes to set
     */
    public void setGenes(String[] genes) {
        this.Genes = genes;
    }

    /**
     * @return the keywords
     */
    public String[] getKeywords() {
        return Keywords;
    }

    /**
     * @param keywords the keywords to set
     */
    public void setKeywords(String[] keywords) {
        this.Keywords = keywords;
    }

    public String getXmlString() {
        Charset charset = Charset.forName("US-ASCII");
        CharsetDecoder decoder = charset.newDecoder();
        CharsetEncoder encoder = charset.newEncoder();
        String s = "";
        

        String retString = new String(
                "<DOC>\n"
                + "<DOCNO>" + this.getDocno() + "</DOCNO>\n"
                + "<PMID>" + this.getPmid() + "</PMID\n"
                + "<TITLE>" + this.getTitle() + "</TITLE>\n"
                + "<ABSTRACT>" + this.getAbstract() + "</ABSTRACT>\n"
                + "<ISSN>" + this.getIssn() + "</ISSN>\n"
                + "<PUBDATE>" + this.getPubdate() + "</PUBDATE>\n"
                + "<SOURCE>" + this.getSource() + "</SOURCE>\n"
                + "<AUTHORS>" + this.getAuthors() + "</AUTHORS>\n"
                + "<CHEMICALS>" + this.getChemicals() + "</CHEMICALS>\n"
                + "<MESH>" + this.getMesh() + "</MESH>\n"
                + "</DOC>");
        try {
            byte[] b = retString.getBytes("UTF-8");
            return new String(b, "US-ASCII");
        } catch (Exception e) {
            System.out.println("getBytes Exception: " + e);
            return retString;
        }

        // FIXME: properly convert to ascii
//        try {
//            // Convert a string to ISO-LATIN-1 bytes in a ByteBuffer
//            // The new ByteBuffer is ready to be read.
//            ByteBuffer bbuf = encoder.encode(CharBuffer.wrap(retString));
//
//            // Convert ISO-LATIN-1 bytes in a ByteBuffer to a character ByteBuffer and then to a string.
//            // The new ByteBuffer is ready to be read.
//            CharBuffer cbuf = decoder.decode(bbuf);
//            s = cbuf.toString();
//        } catch (UnmappableCharacterException e) {
//            System.out.println("Unmappable Ecoding error: " + e);
//            return retString;
//        } catch (CharacterCodingException e) {
//            System.out.println("Ecoding error: " + e);
//            //return retString;
//
//        }
//        return s;
    }

    public String getAnnotatorXmlString() {
        String retString = new String(
                "<DOC>\n"
                + "<DOCNO>" + this.getDocno() + "</DOCNO>\n"
                + "<TEXT>" + this.getTitle()
                + "\n"
                + this.getAbstract() + "</TEXT>\n"
                + "</DOC>");
        return retString;
    }
}
