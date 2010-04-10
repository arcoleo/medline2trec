package Misc;

import java.io.*;

public class XMLFileFilter implements FileFilter {

    private final String[] okFileExtensions =
            new String[]{"xml.gz"};

    public boolean accept(File file) {
        for (String extension : okFileExtensions) {
            if (file.getName().toLowerCase().endsWith(extension)) {
                return true;
            }
        }
        return false;
    }
}
