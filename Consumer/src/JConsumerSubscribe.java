
import java.io.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.IOException;

import com.alibaba.fastjson.JSONObject;

import kafkaProc.ZhiShuDeserilizer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import wangzitian.realtime.ZhiShu;


public class JConsumerSubscribe extends Thread {

    public void run() {
        String topic_name = "SZ399300_zhishu_hangqing";
//        String topic_name = "SH000300_zhishu_hangqing";
//        String topic_name = "SZ000001_hangqing";
        SimpleDateFormat sdf = new SimpleDateFormat("MMddHHmmss");
        Date time_now = new Date();
        String now_str = sdf.format(time_now);

        /* 接收数据（消费端） */
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.101:9092,192.168.1.101:9091,192.168.1.101:9093");
        props.put("group.id", topic_name+"_"+now_str);  //配置client id
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest"); //"earliest" "latest"
        props.put("receive.buffer.bytes", 65536 * 15);
        props.put("socket.receive.buffer.bytes", 64 * 1024 * 10);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", ZhiShuDeserilizer.class.getName());    //指数行情反序列化
        //创建一个消费者对象
        KafkaConsumer<String, ZhiShu> consumer = new KafkaConsumer<String, ZhiShu>(props);
        consumer.subscribe(Arrays.asList(topic_name));  //订阅codes;
        boolean flag = true;
//        try {
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddhhmmssSSS");

        while (flag) {
            //获取主题消息数据
            ConsumerRecords<String, ZhiShu> records = consumer.poll(10);  // ZhiShu
            //            System.out.println("After Poll.");
            for (ConsumerRecord<String, ZhiShu> record : records) {  // ZhiShu

                String code = record.value().getSymbol().toString();  //获得标的代号（例如指数代码）？
                Date date = new Date(record.timestamp());
                System.out.print(sdf2.format(date));
                System.out.println(record.value());

//                JSONObject object = new JSONObject();
//                object.put("LocalTimeStamp", record.value().getLocalTimeStamp());
//                object.put("QuotationFlag", record.value().getQuotationFlag());
//                object.put("getTime", record.value().getTime());
//                object.put("Symbol", record.value().getSymbol());
//                object.put("PreClosePrice", record.value().getPreClosePrice());
//                object.put("OpenPrice", record.value().getOpenPrice());
//                object.put("HighPrice", record.value().getHighPrice());
//                object.put("LowPrice", record.value().getLowPrice());
//                object.put("LastPrice", record.value().getLastPrice());
//                object.put("PacketTimeStampSH", record.value().getPacketTimeStampSH());
//                object.put("TradeTimeSH", record.value().getTradeTimeSH());
//                object.put("TotalAmountSH", record.value().getTotalAmountSH());
//                object.put("TotalNoSZ", record.value().getTotalNoSZ());
//                object.put("TotalVolumeSH", record.value().getTotalVolumeSH());
//                object.put("TotalVolumeSZ", record.value().getTotalVolumeSZ());
//                object.put("ClosePriceSH", record.value().getClosePriceSH());
//                object.put("SymbolSourceSZ", record.value().getSymbolSourceSZ());
//                object.put("SecurityPhaseTagSZ", record.value().getSecurityPhaseTagSZ());
//                object.put("SampleNoSZ", record.value().getSampleNoSZ());
//                System.out.println(object);

                String data = "";
                data += record.value().getLocalTimeStamp() + ";";
                data += record.value().getQuotationFlag() + ";";
                data += record.value().getTime() + ";";
                data += record.value().getSymbol() + ";";
                data += record.value().getPreClosePrice() + ";";
                data += record.value().getOpenPrice() + ";";
                data += record.value().getHighPrice() + ";";
                data += record.value().getLowPrice() + ";";
                data += record.value().getLastPrice() + ";";
                data += record.value().getPacketTimeStampSH() + ";";
                data += record.value().getTradeTimeSH() + ";";
                data += record.value().getTotalAmountSH() + ";";
                data += record.value().getTotalNoSZ() + ";";
                data += record.value().getTotalVolumeSH() + ";";
                data += record.value().getTotalVolumeSZ() + ";";
                data += record.value().getClosePriceSH() + ";";
                data += record.value().getSymbolSourceSZ() + ";";
                data += record.value().getSecurityPhaseTagSZ() + ";";
                data += record.value().getSampleNoSZ() + ";";
                System.out.println(data);

                /*数据转发*/
                ProducerRecord<String, String> record_string = new ProducerRecord<>(topic_name,data);
                try{
                    Kafka.zhishuhangqingOnly_producer.send(record_string, new ZhiShuHangQingProducerCallback());
                    System.out.println("sending");
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("code send to kafka failed  " + code);
                }
            }
        }
        consumer.close();

//
    }

}