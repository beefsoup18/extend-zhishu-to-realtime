import java.util.*;
import java.util.concurrent.ExecutionException;

import kafkaProc.HangQingSerializer;
import kafkaProc.OrderSerializer;
import kafkaProc.TransacSerilizer;
import kafkaProc.ZhiShuHangQingSerializer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import wangzitian.realtime.SZorder;
import wangzitian.realtime.Transactions;
import wangzitian.realtime.HangQing;
import wangzitian.realtime.ZhiShu;


public class Kafka {
//    public static String boker_url = new String("192.168.1.101:9092,192.168.1.101:9091,192.168.1.101:9093");
//    public static String boker_url = new String("127.0.0.1:9092");
    public static String second_boker_url = new String("192.168.1.101:9092,192.168.1.101:9091,192.168.1.101:9093");
//    public static String boker_url = new String("192.168.1.101:9092,192.168.1.101:9091,192.168.1.101:9093");
    public static String boker_url = new String(second_boker_url);//("192.168.1.101:9092,192.168.1.101:9091,192.168.1.101:9093");
    public static ArrayList<KafkaProducer<String, Transactions>> producers = new ArrayList<KafkaProducer<String, Transactions>>();
    public static ArrayList<KafkaProducer<String, SZorder>> OrderProducers = new ArrayList<KafkaProducer<String, SZorder>>();

    public static KafkaProducer<String, Transactions> single_producer = null;
    public static KafkaProducer<String, SZorder> orderOnly_producer = null;
    public static KafkaProducer<String, HangQing> hangqingOnly_producer = null;
    public static KafkaProducer<String, ZhiShu> zhishuhangqingOnly_producer = null;

//    public static Lock backup_lock = new ReentrantLock();
    public static void createTopic() throws ExecutionException, InterruptedException {
        short rep_num = 1;
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.boker_url);
        AdminClient client0 = AdminClient.create(props);
        for(int i=0;i<Codes.code_talbe_names.size();i++) {
            try {
                NewTopic newTopic = new NewTopic(Codes.code_talbe_names.get(i),
                        1, rep_num);
                CreateTopicsResult ret = client0.createTopics(Arrays.asList(newTopic));
                ret.all().get();
                System.out.println(i + "th topic:" + Codes.code_talbe_names.get(i) + " created.");

                NewTopic OrdernewTopic = new NewTopic(Codes.code_talbe_names.get(i) + "_order",
                        1, rep_num);
                CreateTopicsResult Orderret = client0.createTopics(Arrays.asList(OrdernewTopic));
                Orderret.all().get();
                System.out.println(i + "th order topic:" + Codes.code_talbe_names.get(i) + "_order" + " created.");

                NewTopic HangQingnewTopic = new NewTopic(Codes.code_talbe_names.get(i) + "_hangqing",
                        1, rep_num);
                CreateTopicsResult Hangqingret = client0.createTopics(Arrays.asList(HangQingnewTopic));
                Hangqingret.all().get();
                System.out.println(i + "th hangqing topic:" + Codes.code_talbe_names.get(i) + "_hangqing" + " created.");

            } catch (Exception e){
                e.printStackTrace();
            }
        }
        ListTopicsOptions options = new ListTopicsOptions();
        ListTopicsResult topics = client0.listTopics(options);
        Set<String> topicNames = topics.names().get();

        System.out.println("After Topic created:\n" + topicNames);

        client0.close();
    }

    public static void deleteTopic() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.boker_url);
        AdminClient client0 = AdminClient.create(props);
        for(int i=0;i<Codes.code_talbe_names.size();i++) {
            try {
                DeleteTopicsResult futures = client0.deleteTopics(Collections.singletonList(Codes.code_talbe_names.get(i)));
                futures.all().get();
                System.out.println(i + "th topic:" + Codes.code_talbe_names.get(i) + " deleted.");

            }catch (Exception e){
                e.printStackTrace();
            }
        }

        ListTopicsOptions options = new ListTopicsOptions();
        ListTopicsResult topics = client0.listTopics(options);
        Set<String> topicNames = topics.names().get();

        System.out.println("After Topic deleted:\n" + topicNames);

        client0.close();
    }

    public static void buildProducers(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.boker_url);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransacSerilizer.class.getName());
        kafkaProps.put("linger.ms", 20);
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

        for(int i=0;i<Codes.thread_num;i++){
            Kafka.producers.add(new KafkaProducer<String, Transactions>(kafkaProps));
            System.out.println(i + "th kafka transaction producer created.");
        }
    }

    public static void buildOrderProducers(){

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.second_boker_url);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());
        kafkaProps.put("linger.ms", 5);
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
        for(int i=0;i<Codes.thread_num;i++){
            Kafka.OrderProducers.add(new KafkaProducer<String, SZorder>(kafkaProps));
            System.out.println(i + "th kafka order producer created.");
        }

    }

    public static void buildHangQingOnlyProducers(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.second_boker_url);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HangQingSerializer.class.getName());
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

        Kafka.hangqingOnly_producer = new KafkaProducer<String, HangQing>(kafkaProps);

    }


    public static void buildOrderOnlyProducers(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.second_boker_url);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());
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

        Kafka.orderOnly_producer = new KafkaProducer<String, SZorder>(kafkaProps);

    }


    public static void buildSingleProducer(){
        /**Kafka中处理成交数据的生产者**/
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.boker_url);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransacSerilizer.class.getName());
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

        Kafka.single_producer = new KafkaProducer<String, Transactions>(kafkaProps);

//        Kafka.backup_initialized.add(false);

    }


    public static void buildZhiShuHangQingOnlyProducers(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.second_boker_url);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ZhiShuHangQingSerializer.class.getName());
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

        Kafka.zhishuhangqingOnly_producer = new KafkaProducer<String, ZhiShu>(kafkaProps);

    }

}
