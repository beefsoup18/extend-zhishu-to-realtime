
import java.io.IOException;
import java.text.ParseException;

public class run {
    public static void main(String args[]) throws IOException, ParseException {

//        Kafka.buildSingleProducer();
//        System.out.println("创建成交数据生产者");
//        Kafka.buildOrderOnlyProducers();
//        System.out.println("创建委托数据生产者");
//        Kafka.buildHangQingOnlyProducers();
//        System.out.println("创建行情数据生产者");
        Kafka.buildZhiShuHangQingOnlyProducers();
        System.out.println("创建指数数据生产者");

        JConsumerSubscribe jconsumer = new JConsumerSubscribe();
//        JConsumer_hangqing jconsumer = new JConsumer_hangqing();
//        JConsumer_transaction jconsumer = new JConsumer_transaction();
        jconsumer.start();
    }
}