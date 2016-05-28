package com.zmyuan.sparkhw.hw06;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zdb on 2016/5/22.
 */
public class SocketLogServer {

    public static void main(String args[]) {

        try {

            ServerSocket server = null;

            try {

                server = new ServerSocket(8818);

                //创建一个ServerSocket在端口8818监听客户请求

            } catch (Exception e) {

                System.out.println("can not listen to:" + e);

                //出错，打印出错信息

            }

            Socket socket = null;

            try {

                socket = server.accept();
                System.out.println("---------连接到socket ----------");

                //使用accept()阻塞等待客户请求，有客户

                //请求到来则产生一个Socket对象，并继续执行

            } catch (Exception e) {

                System.out.println("Error." + e);

                //出错，打印出错信息

            }

            // 阻塞到socket建立
            File file = new File("/Users/zhudebin/Documents/iworkspace/opensource/spark-source-homework/lesson-6/docs/hh.log");
            long lastModifed = 0;
            //由Socket对象得到输入流，并构造相应的BufferedReader对象
            PrintWriter os = new PrintWriter(socket.getOutputStream());
            long lastestRead = 0;
            while(true) {
                Thread.sleep(1000);
                if(file.lastModified() - lastModifed > 0) {
                    System.out.println("----------- modified ----------");

                    // 读取文件新增内容
                    byte[] bytes = FileUtil.readFile(file, lastestRead);
                    if(bytes == null || bytes.length == 0) {
                        if(lastModifed == 0) {
                            lastModifed = file.lastModified();
                        }
                        continue;
                    }
                    int endLinePos = 0;
                    for(int i=0; i<bytes.length; i++) {
                        if(bytes[i] == '\n') {
                            String line = new String(bytes, endLinePos, i-endLinePos);
                            endLinePos = i+1;
                            System.out.println("--send socket-----line-------" + line);
                            // send to socket
                            os.println(line);
                            os.flush();
                        }
                    }
                    lastestRead += endLinePos-1;
                    lastModifed = file.lastModified();
                }
            }


//            os.close(); //关闭Socket输出流
//
//            socket.close(); //关闭Socket
//
//            server.close(); //关闭ServerSocket

        } catch (Exception e) {
            e.printStackTrace();
        }



    }


}
