package org.kang;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.kang.util.MySQLManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 第一题
 *      计算结果存储于MySQL中, 可以利用MySQL的事务, 将offset保存于MySQL中, 保证exactly-once.
 *      Kafka Topic数据结构: host|uri|content
 *
 * @author Kang
 * @version 1.0
 * @time 2019-07-21 10:02
 */
public class FirstApp implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirstApp.class);

    public static void main(String[] args) throws Exception {
        new FirstApp().run();
    }

    private void run() throws Exception {
        try(
            JavaStreamingContext jsc = createJavaStreamingContext();
        ) {
            String topic = "test";
            String groupId = "test_group_id";
            Map<String, Object> kafkaParamMap = createKafkaParamMap(groupId);

            // 从MySQL中查询offset
            Map<TopicPartition, Long> fromOffsetMap = getTopicPartitionOffsetMap(groupId, topic);

            // 指定offset, 从Kafka中消费数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            jsc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(Sets.newHashSet(topic), kafkaParamMap, fromOffsetMap)
                    );

            stream.foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) rdd -> {
                if (rdd.isEmpty()) return;

                // 获得当前消息的offset信息
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                rdd.foreachPartition((VoidFunction<Iterator<ConsumerRecord<String, String>>>) consumerRecordIterator -> {

                    Connection mysqlConn = null;
                    PreparedStatement updateTestPstmt = null;
                    PreparedStatement updateKafkaOffsetPstmt = null;
                    try{
                        // 获得MySQL连接
                        mysqlConn = MySQLManager.getConnection();

                        // 业务处理SQL: 是否和Mysql里面的数据host, uri, content是否重复。重复则更新计算器和时间，不重复则直接写入数据
                        // host、uri、content组成id主键列
                        String updateTestSQL = "INSERT INTO test (id,host,uri,content,count,ctime) VALUES (?,?,?,?,1,now()) " +
                                "ON DUPLICATE KEY UPDATE count = count + 1, ctime = now()";
                        updateTestPstmt = mysqlConn.prepareStatement(updateTestSQL);
                        while (consumerRecordIterator.hasNext()) {
                            ConsumerRecord<String, String> consumerRecord = consumerRecordIterator.next();

                            // 消息内容
                            String value = consumerRecord.value();

                            // value为null或空白字符, 跳过
                            if (StringUtils.isBlank(value)) continue;

                            // Kafka Topic数据结构: host|uri|content
                            String[] fields = value.split("\\|");

                            // 消息格式有误, 跳过
                            if (fields.length != 3) continue;

                            String host = fields[0];
                            String uri = fields[1];
                            String content = fields[2];

                            updateTestPstmt.setString(1, host.concat(uri).concat(content));
                            updateTestPstmt.setString(2, host);
                            updateTestPstmt.setString(3, uri);
                            updateTestPstmt.setString(4, content);

                            updateTestPstmt.addBatch();
                        }

                        // offset处理
                        String updateKafkaOffsetSQL = "REPLACE INTO kafka_offset(id, group_id, topic, `partition`, `offset`) VALUES(?,?,?,?,?)";
                        updateKafkaOffsetPstmt = mysqlConn.prepareStatement(updateKafkaOffsetSQL);
                        // id = group_id|topic|partition
                        String sep = "|";
                        String idPrefix = new StringBuilder(groupId)
                                .append(sep)
                                .append(topic)
                                .append(sep)
                                .toString();
                        for (OffsetRange offsetRange : offsetRanges) {
                            updateKafkaOffsetPstmt.setString(1, idPrefix + (offsetRange.partition()));
                            updateKafkaOffsetPstmt.setString(2, groupId);
                            updateKafkaOffsetPstmt.setString(3, offsetRange.topic());
                            updateKafkaOffsetPstmt.setInt(4, offsetRange.partition());
                            updateKafkaOffsetPstmt.setLong(5, offsetRange.untilOffset());

                            updateKafkaOffsetPstmt.addBatch();
                        }

                        // 设置手动提交
                        mysqlConn.setAutoCommit(false);

                        // 更新test表
                        updateTestPstmt.executeBatch();
                        // 更新kafka_offset表
                        updateKafkaOffsetPstmt.executeBatch();

                        mysqlConn.commit();
                    } finally {
                        // 资源释放
                        MySQLManager.release(updateKafkaOffsetPstmt, updateTestPstmt, mysqlConn);
                    }
                });
            });

            jsc.start();
            jsc.awaitTermination();
        }
    }

    /**
     * 创建JavaStreamingContext对象
     *
     * @return JavaStreamingContext对象
     */
    private JavaStreamingContext createJavaStreamingContext(){
        SparkConf sparkConf = new SparkConf()
                .setIfMissing("spark.app.name", this.getClass().getSimpleName())
                .setIfMissing("spark.master", "local[*]")
                .setIfMissing("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        return jssc;
    }

    /**
     * 创建Kakfa消费者参数Map
     *
     * @param groupId 消费者组ID
     * @return Kakfa消费者参数Map
     */
    private Map<String, Object> createKafkaParamMap(String groupId){
        Map<String, Object> kafkaParamMap = new HashMap<>();
        kafkaParamMap.put("bootstrap.servers", "localhost:9092");
        kafkaParamMap.put("key.deserializer", StringDeserializer.class);
        kafkaParamMap.put("value.deserializer", StringDeserializer.class);
        kafkaParamMap.put("group.id", groupId);
        kafkaParamMap.put("auto.offset.reset", "latest");
        kafkaParamMap.put("enable.auto.commit", false);

        return kafkaParamMap;
    }

    /**
     * 查询指定Topic的所有分区的offset
     * @param topic
     * @return
     * @throws SQLException
     */
    private Map<TopicPartition, Long> getTopicPartitionOffsetMap(String groupId, String topic) throws SQLException {
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        Connection mysqlConn = null;

        Map<TopicPartition, Long> fromOffsetMap;
        try{
            // 获得MySQL连接
            mysqlConn = MySQLManager.getConnection();
            // 根据topic名称查询所有partition的offset
            String sql = "SELECT * FROM kafka_offset WHERE group_id = ? AND topic = ?";
            preparedStatement = mysqlConn.prepareStatement(sql);
            preparedStatement.setString(1, groupId);
            preparedStatement.setString(2, topic);
            resultSet = preparedStatement.executeQuery();

            fromOffsetMap = new HashMap<>();
            while(resultSet.next()){
                int partition = resultSet.getInt("partition");
                // 从下一个offset开始消费
                long offset = resultSet.getLong("offset") + 1;

                fromOffsetMap.put(
                        new TopicPartition(topic, partition), offset);
            }
        } finally {
            MySQLManager.release(resultSet, preparedStatement, mysqlConn);
        }

        return fromOffsetMap;
    }
}