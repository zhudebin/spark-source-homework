package com.zmyuan.sparkhw.hw06;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhudebin on 16/5/28.
 */
public class CsvParserTest {

    @Test
    public void test1() throws IOException {
        String str = "192.168.72.177 - - [22/Dec/2002:23:32:19 -0400] \"GET /search.php HTTP/1.1\" 400 1997 www.yahoo.com \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ...)\" \"-\"";
        List<CSVRecord> list = CSVParser.parse(str, CSVFormat.MYSQL).getRecords();
        for(CSVRecord cr : list) {
            System.out.println(cr);
        }
    }

    @Test
    public void test2() {
        String str = "192.168.72.177 - - [22/Dec/2002:23:32:19 -0400] \"GET /search.php HTTP/1.1\" 400 1997 www.yahoo.com \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ...)\" \"-\"";
        String[] strs = str.split(" ");
        System.out.println(strs);
    }

}
