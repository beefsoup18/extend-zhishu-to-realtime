package kafkaProc;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerCallback implements Callback{

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception){
        if(exception != null) {
            exception.printStackTrace();
        }
        else {
            System.out.println("send signal complete.");
        }

    }


}
