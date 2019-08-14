import java.util.*;
import java.util.concurrent.ExecutionException;


import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;



public class Kafka {

    public static String second_boker_url = new String("192.168.1.102：9092");  //"118.89.243.189:9092,118.89.243.189:9093,118.89.243.189:9094"
    public static String boker_url = new String(second_boker_url);//("192.168.1.101:9092,192.168.1.101:9091,192.168.1.101:9093");
//    public static ArrayList<KafkaProducer<String, Transactions>> producers = new ArrayList<KafkaProducer<String, Transactions>>();
//    public static ArrayList<KafkaProducer<String, SZorder>> OrderProducers = new ArrayList<KafkaProducer<String, SZorder>>();
//
    public static KafkaProducer<String, String> single_producer = null;
    public static KafkaProducer<String, String> orderOnly_producer = null;
    public static KafkaProducer<String, String> hangqingOnly_producer = null;
    public static KafkaProducer<String, String> zhishuhangqingOnly_producer = null;


    public static void buildHangQingOnlyProducers(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.second_boker_url);
//        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HangQingSerializer.class.getName());
        kafkaProps.put("linger.ms", 15);
//        kafkaProps.put("compression.type", "gzip");
        kafkaProps.put("buffer.memory", Long.valueOf("335544320")*5);
        kafkaProps.put("send.buffer.bytes", 131072*15);
        kafkaProps.put("receive.buffer.bytes", 32678*15);
        kafkaProps.put("request.timeout.ms", 45000);

        kafkaProps.put("acks", "1");
//        kafkaProps.put("batch.size", 500);
        kafkaProps.put("retry.backoff.ms", 250);
        kafkaProps.put("retries", Integer.MAX_VALUE);
//        kafkaProps.put("retries", 1);
        kafkaProps.put("max.block.ms", Long.MAX_VALUE);
        kafkaProps.put("max.in.flight.requests.per.connection", 1);
        kafkaProps.put("log.message.timestamp.type", "LogAppendTime");

        Kafka.hangqingOnly_producer = new KafkaProducer<>(kafkaProps);  //Zhishu

    }


    public static void buildOrderOnlyProducers(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.second_boker_url);
//        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());
        kafkaProps.put("linger.ms", 15);
        kafkaProps.put("compression.type", "gzip");
        kafkaProps.put("buffer.memory", Long.valueOf("335544320")*5);
        kafkaProps.put("send.buffer.bytes", 131072*10);
        kafkaProps.put("receive.buffer.bytes", 32678*7);
        kafkaProps.put("request.timeout.ms", 45000);


        kafkaProps.put("acks", "1");
//        kafkaProps.put("batch.size", 500);
        kafkaProps.put("retry.backoff.ms", 250);
        kafkaProps.put("retries", Integer.MAX_VALUE);
        kafkaProps.put("max.block.ms", Long.MAX_VALUE);
        kafkaProps.put("max.in.flight.requests.per.connection", 1);
        kafkaProps.put("log.message.timestamp.type", "LogAppendTime");

        Kafka.orderOnly_producer = new KafkaProducer<>(kafkaProps);
    }


    public static void buildSingleProducer(){
        /**Kafka中处理成交数据的生产者**/
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.boker_url);
//        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransacSerilizer.class.getName());
        kafkaProps.put("linger.ms", 15);
        kafkaProps.put("compression.type", "gzip");
        kafkaProps.put("buffer.memory", Long.valueOf("335544320")*10);
        kafkaProps.put("send.buffer.bytes", 131072*20);
        kafkaProps.put("receive.buffer.bytes", 32678*15);
        kafkaProps.put("request.timeout.ms", 45000);


        kafkaProps.put("acks", "1");
//        kafkaProps.put("batch.size", 500);
        kafkaProps.put("retry.backoff.ms", 250);
        kafkaProps.put("retries", Integer.MAX_VALUE);
        kafkaProps.put("max.block.ms", Long.MAX_VALUE);
        kafkaProps.put("max.in.flight.requests.per.connection", 1);
        kafkaProps.put("log.message.timestamp.type", "LogAppendTime");

        Kafka.single_producer = new KafkaProducer<>(kafkaProps);
    }


    public static void buildZhiShuHangQingOnlyProducers(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.second_boker_url);
//        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ZhiShuHangQingSerializer.class.getName());
        kafkaProps.put("linger.ms", 15);
//        kafkaProps.put("compression.type", "gzip");
        kafkaProps.put("buffer.memory", Long.valueOf("335544320")*5);
        kafkaProps.put("send.buffer.bytes", 131072*15);
        kafkaProps.put("receive.buffer.bytes", 32678*15);
        kafkaProps.put("request.timeout.ms", 45000);

        kafkaProps.put("acks", "1");
//        kafkaProps.put("batch.size", 500);
        kafkaProps.put("retry.backoff.ms", 250);
        kafkaProps.put("retries", Integer.MAX_VALUE);
//        kafkaProps.put("retries", 1);
        kafkaProps.put("max.block.ms", Long.MAX_VALUE);
        kafkaProps.put("max.in.flight.requests.per.connection", 1);
        kafkaProps.put("log.message.timestamp.type", "LogAppendTime");

        Kafka.zhishuhangqingOnly_producer = new KafkaProducer<>(kafkaProps);

    }

}
