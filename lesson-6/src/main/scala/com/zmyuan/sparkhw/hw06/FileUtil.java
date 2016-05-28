package com.zmyuan.sparkhw.hw06;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by zhudebin on 16/5/28.
 */
public class FileUtil {

    public static boolean isModified(File file, long lastModified) {
        if(lastModified != 0) {
            if(file.lastModified() - lastModified > 0) {
                lastModified = file.lastModified();
                return true;
            }
        } else {
            lastModified = file.lastModified();
        }
        return false;
    }

    public static byte[] readFile(File file, long startPosition) throws Exception {

        if(file.length() <= startPosition) {
            return null;
        }

        RandomAccessFile in = new RandomAccessFile(file, "r");

        in.seek(startPosition);
        byte[] buffer = new byte[1024 * 3];


        int cnt = in.read(buffer);

        for(int i=cnt-1; i>0; i--) {
            if(buffer[i] == '\n') {
                // 获取换行的最后一个位置
                cnt = i+1;
                break;
            }
        }

        return Arrays.copyOfRange(buffer, 0, cnt);
    }

}
