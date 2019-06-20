package flink.utils;

import java.io.File;
import java.io.IOException;

public class FileUtils {
    public static void createFile(String[] pathlist) throws IOException {
        for(String path:pathlist){
            File file=new File(path);
            file.createNewFile();
        }

    }
}
