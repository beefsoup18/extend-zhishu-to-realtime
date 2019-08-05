
import java.io.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;

import kafkaProc.HangQingDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import wangzitian.realtime.HangQing;


public class JConsumer_hangqing extends Thread {

    public void run() {

        String topic_name = "SZ000006_hangqing";
        SimpleDateFormat sdf = new SimpleDateFormat("MMddHHmmss");
        Date time_now = new Date();
        String now_str = sdf.format(time_now);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", topic_name+"_"+now_str);  //配置client id
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest"); //"earliest"
        props.put("receive.buffer.bytes", 65536 * 15);
        props.put("socket.receive.buffer.bytes", 64 * 1024 * 10);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", HangQingDeserializer.class.getName());    //指数行情反序列化
        //创建一个消费者对象
        KafkaConsumer<String, HangQing> consumer = new KafkaConsumer<String, HangQing>(props);
        consumer.subscribe(Arrays.asList(topic_name));  //订阅codes;
        boolean flag = true;

        while (flag) {
            //获取主题消息数据
            ConsumerRecords<String, HangQing> records = consumer.poll(10);  // ZhiShu
            //            System.out.println("After Poll.");
            for (ConsumerRecord<String, HangQing> record : records) {  // ZhiShu
                String code = record.value().getSymbol().toString();  //获得标的代号（例如指数代码）？
                System.out.println(record.value());

            }
//            System.out.println(file.getAbsoluteFile());
        }
        consumer.close();

//
    }

}