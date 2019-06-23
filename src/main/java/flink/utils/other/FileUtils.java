package flink.utils.other;

import java.io.File;
import java.io.IOException;

public class FileUtils {
    public static void createFile(String[] pathlist) throws IOException {
        for(String path:pathlist){
            File file=new File(path);
            file.createNewFile();
        }

    }

    public static String getId(String file) {
        switch (file){
            case "rankhourly.csv":
                return "H";
            case "rankdaily.csv":
                return "D";
            case "rankweekly.csv":
                return "W";
            case "popdaily.csv":
                return "D";
            case "popweekly.csv":
                return "W";
            case "popmonthly.csv":
                return "M";
            default:
                return null;
        }
    }
}
