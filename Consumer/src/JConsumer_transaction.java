
import java.io.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;

import kafkaProc.TransacDeserilaizer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import wangzitian.realtime.Transactions;


public class JConsumer_transaction extends Thread {

    public void run() {

        String topic_name = "SZ300392";
        SimpleDateFormat sdf = new SimpleDateFormat("MMddHHmmss");
        Date time_now = new Date();
        String now_str = sdf.format(time_now);

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.103:9092");
        props.put("group.id", topic_name+"_"+now_str);  //配置client id
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest"); //"earliest"  "latest"
        props.put("receive.buffer.bytes", 65536 * 15);
        props.put("socket.receive.buffer.bytes", 64 * 1024 * 10);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", TransacDeserilaizer.class.getName());    //指数行情反序列化
        //创建一个消费者对象
        KafkaConsumer<String, Transactions> consumer = new KafkaConsumer<String, Transactions>(props);
        consumer.subscribe(Arrays.asList(topic_name));  //订阅codes;
        boolean flag = true;
        try {

            File file = new File(topic_name+".txt");
            SimpleDateFormat sdf2 =new SimpleDateFormat("yyyyMMddhhmmssSSS");
            while (flag) {
                //获取主题消息数据
                ConsumerRecords<String, Transactions> records = consumer.poll(10);  // ZhiShu
                //            System.out.println("After Poll.");
                for (ConsumerRecord<String, Transactions> record : records) {  // ZhiShu
                    String code = record.value().getSymbol().toString();  //获得标的代号（例如指数代码）？
                    System.out.println(record.value());
                    if(!file.exists()){
                        file.createNewFile();}
                    FileWriter fw = new FileWriter(file, true);
                    BufferedWriter bw = new BufferedWriter(fw);

                    bw.write(record.value().getLocalTimeStamp() + "; ");
                    bw.write(record.value().getQuotationFlag() + "; ");
                    bw.write(record.value().getPacketTimeStampSH() + "; ");
                    bw.write(record.value().getBuyID() + "; ");
                    bw.write(record.value().getSellID() + "; ");
                    bw.write(record.value().getBuyID() + "; ");
                    bw.write(record.value().getTradePrice() + "; ");
                    bw.write(record.value().getTradeVolume() + "; ");
                    bw.write(record.value().getTradeAmount() + "; ");
                    bw.write(record.value().getRecID() + "; ");
//                    bw.write(record.value().getTradeChannelSH() + "; ");
                    bw.newLine();
                    bw.flush();
                    bw.close();
                    fw.close();
                }
    //            System.out.println(file.getAbsoluteFile());
            }
            consumer.close();
        } catch(IOException e){
            e.printStackTrace();
        }
//
    }

}