package generateindices;

import Medline.*;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import org.apache.log4j.*;

public class Trec {

    static Logger logger = Logger.getLogger(Trec.class);
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

    public String clean(String str) {
        return str;
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
            logger.error(ex);
            this.Docno = "";
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
            logger.error(ex);
            this.Pmid = "";
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
            logger.error(ex);
            this.Source = "";
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
            logger.error(ex);
            this.Title = "";
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
            Abstract = "";
            try {
                for (OtherAbstract d : currCitation.getOtherAbstract()) {
                    if (d.getAbstractText().length() > 5) {
                        this.Abstract += d.getAbstractText();
                    }
                }
            } catch (Exception e) {
                logger.error("OtherAbstract Exception: " + e);
            }
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
            // NOTE: we don't log this exception because it's so common
            this.Issn = "";
        }
    }

    /**
     * @return the Chemicals
     */
    public String getChemicals() {
        String chems = "";
        for (String chemical : Chemicals) {
            chems += chemical + " : ";
        }
        if (chems.length() > 5) {
            chems = chems.substring(0, chems.length() - 3);
        }
        return chems;
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
                logger.error(ex);
            }
        }
    }

    /**
     * @return the Mesh
     */
    public String getMesh() {
        String mesh_str = "";
        for (String currMesh : Mesh) {
            mesh_str += currMesh + " : ";
        }
        if (mesh_str.length() > 5) {
            mesh_str = mesh_str.substring(0, mesh_str.length() - 3);
        }
        return mesh_str;
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
                logger.error(ex);
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
        Pubdate = "";
        try {
            for (Object d : currCitation.getArticle().getJournal().getJournalIssue().getPubDate().getYearOrMonthOrDayOrSeasonOrMedlineDate()) {
                if (d instanceof Medline.Year) {
                    this.Pubdate += ((Medline.Year) d).getvalue() + " ";
                } else if (d instanceof Medline.Month) {
                    this.Pubdate += ((Medline.Month) d).getvalue() + " ";
                }
            }
            Pubdate = Pubdate.trim();
        } catch (Exception ex) {
            logger.error(ex);
        }
    }

    /**
     * @return the Authors
     */
    public String getAuthors() {
        //return Authors;
        String auth = "";
        for (String author : Authors) {
            auth += author + " : ";
        }
        if (auth.length() > 5) {
            auth = auth.substring(0, auth.length() - 3);
        }
        return auth;
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
                logger.error(ex);
            }
        }

        // remove separator
        this.Authors.remove(this.Authors.size() - 1);
        this.Authors.add(fname + " " + lname);

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
        String s = "";


        String retString = new String(
                "<DOC>\n"
                + "<DOCNO>" + this.getDocno() + "</DOCNO>\n"
                + "<PMID>" + this.getPmid() + "</PMID>\n"
                + "<TITLE>" + this.clean(this.getTitle()) + "</TITLE>\n"
                + "<ABSTRACT>" + this.clean(this.getAbstract()) + "</ABSTRACT>\n"
                + "<ISSN>" + this.getIssn() + "</ISSN>\n"
                + "<PUBDATE>" + this.getPubdate() + "</PUBDATE>\n"
                + "<SOURCE>" + this.clean(this.getSource()) + "</SOURCE>\n"
                + "<AUTHOR>" + this.getAuthors() + "</AUTHOR>\n"
                + "<CHEMICALS>" + this.getChemicals().toString() + "</CHEMICALS>\n"
                + "<MESH>" + this.getMesh() + "</MESH>\n"
                + "</DOC>\n");
        try {
            // FIXME: possibly get rid of conversion?
            byte[] b = retString.getBytes("UTF-8");
            return new String(b, "US-ASCII");
        } catch (Exception ex) {
            logger.error(ex);
            return retString;
        }
    }

    public String getAnnotatorXmlString() {
        String retString = new String(
                "<DOC>\n"
                + "<DOCNO>" + this.getDocno() + "</DOCNO>\n"
                + "<TEXT>" + this.getTitle()
                + "\n"
                + this.getAbstract() + "</TEXT>\n"
                + "</DOC>\n");
        return retString;
    }
}
