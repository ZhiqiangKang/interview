package org.kang;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.List;

/**
 * 第二题: Spark版、Java单机版
 *
 * @author Kang
 * @version 1.0
 * @time 2019-07-21 10:50
 */
public class SecondApp implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecondApp.class);

    public static void main(String[] args) {
//        new SecondApp().run();
        new SecondApp().runLocal();
    }

    /**
     * Spark版
     */
    private void run(){
        SparkSession sparkSession = createSparkSession();

        // 读取CSV格式源文件为Dataset<Row>
        Dataset<Row> contentRowDS = sparkSession.read().csv("data/csv.txt");

        Dataset<String> resultRowDS = contentRowDS.map((MapFunction<Row, String>) row -> {
            String c0 = row.getString(0);
            String c1 = formatString(row.getString(1));
            String c2 = formatString(row.getString(2));
            String c3 = row.getString(3);
            String c4 = formatDate(row.getString(4));

            // 字段之间以 TAB 分隔
            return Joiner.on('\t').join(c0, c1, c2, c3, c4);
        }, Encoders.STRING());

        // 保存至文件夹
        resultRowDS.write().text("data/output");
    }

    /**
     * Java单机版
     */
    private void runLocal(){
        try (CSVParser csvParser = CSVParser.parse(new File("data/csv.txt"),
                Charset.defaultCharset(), CSVFormat.INFORMIX_UNLOAD_CSV)) {

            StringBuilder resultSB = new StringBuilder();
            File resultFile = new File("data/output.txt");

            List<CSVRecord> csvRecordList = csvParser.getRecords();
            int csvRecordListSize = csvRecordList.size();
            for (int i = 0; i < csvRecordListSize; i++){
                CSVRecord csvRecord = csvRecordList.get(i);

                String c0 = csvRecord.get(0);
                String c1 = formatString(csvRecord.get(1));
                String c2 = formatString(csvRecord.get(2));
                String c3 = csvRecord.get(3);
                String c4 = formatDate(csvRecord.get(4));

                // 字段之间以 TAB 分隔
                resultSB.append(Joiner.on('\t').join(c0, c1, c2, c3, c4, '\n'));

                if (i % 200 == 0 || i == csvRecordListSize - 1){

                    // 追加至文件
                    Files.append(resultSB, resultFile, Charset.defaultCharset());
                    resultSB.setLength(0);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建SparkSession对象
     *
     * @return SparkSession对象
     */
    private SparkSession createSparkSession(){
        SparkConf sparkConf = new SparkConf()
                .setIfMissing("spark.app.name", this.getClass().getSimpleName())
                .setIfMissing("spark.master", "local[*]");

        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        return sparkSession;
    }

    /**
     * 格式化字符串字段
     *
     * @param str 待格式化的字符串
     * @return 格式化的字符串
     */
    private String formatString(String str){
        if (str == null) return null;

        // 替换""为"
        String rtnValue = str.replace("\"\"", "\"");

        if (rtnValue.startsWith("\"")){
            // 开头是", 则结尾一定有", 直接删除两端的"
            // 示例: "下棋,飞"    ""下棋",飞"   "下棋,"飞""
            rtnValue = rtnValue.substring(1, rtnValue.length() - 1);

        }

        // 字符串字段用'括起来
        rtnValue = "'".concat(rtnValue).concat("'");

        return rtnValue;
    }

    /**
     * 格式化日期字段
     *
     * @param dateStr 待格式化的字符串
     * @return 格式化的字符串
     */
    private String formatDate(String dateStr){
        if (dateStr == null) return null;

        String str = null;
        try {
            str = DateFormatUtils.format(
                    DateUtils.parseDate(dateStr, "yyyy-MM-dd"), "yyyy/MM/dd");
        } catch (ParseException e) {
            LOGGER.warn("日期[{}]格式转换错误", dateStr);
        }
        return str;
    }
}