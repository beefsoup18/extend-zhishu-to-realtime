import org.apache.kafka.clients.producer.KafkaProducer;
import wangzitian.realtime.AvroSigalSerial;
import wangzitian.realtime.SignalRange0;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
public class KafkaSend {
    public static String sendBoker_url = new String("118.89.243.189:9092,118.89.243.189:9093,118.89.243.189:9094");
    public static KafkaProducer<String, SignalRange0> single_producer = null;

    public static void buildSingleProducer(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSend.sendBoker_url);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSigalSerial.class.getName());
        kafkaProps.put("retries", 1000000000);
        kafkaProps.put("max.in.flight.requests.per.connection", 1);
        kafkaProps.put("log.message.timestamp.type", "LogAppendTime");
        KafkaSend.single_producer = new KafkaProducer<String, SignalRange0>(kafkaProps);
    }



}
