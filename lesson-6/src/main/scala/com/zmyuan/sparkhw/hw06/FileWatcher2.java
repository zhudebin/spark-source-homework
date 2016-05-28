package com.zmyuan.sparkhw.hw06;

import org.apache.commons.io.DirectoryWalker;

import java.io.File;
import java.io.PrintWriter;

/**
 * Created by zhudebin on 16/5/28.
 */
public class FileWatcher2 {

    public static void main(String[] args) throws Exception {

        // 阻塞到socket建立
        File file = new File("/Users/zhudebin/Documents/iworkspace/opensource/spark-source-homework/lesson-6/docs/hh.log");
        long lastModifed = 0;
        long lastestRead = 0;
        while(true) {

            if(file.lastModified() - lastModifed > 0) {
                System.out.println("----------- modified ----------");

                // 读取文件新增内容
                byte[] bytes = FileUtil.readFile(file, lastestRead);
                int endLinePos = 0;
                for(int i=0; i<bytes.length; i++) {
                    if(bytes[i] == '\n') {
                        String line = new String(bytes, endLinePos, i-endLinePos);
                        endLinePos = i+1;
                        System.out.println("--send socket-----line-------" + line);
                    }
                }
                lastestRead += endLinePos-1;
                lastModifed = file.lastModified();
            }
        }

    }

}
