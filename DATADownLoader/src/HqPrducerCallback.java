
/**行情的Kafka生产者**/

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import wangzitian.realtime.HangQing;

public class HqPrducerCallback implements Callback{

    public HqPrducerCallback(){

    }
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception){
        if(exception != null) {
            exception.printStackTrace();
            try {
                FileWriter fstream = new FileWriter("./callbackException_hangqing.txt", true);
                BufferedWriter out = new BufferedWriter(fstream);
                out.write(exception.toString());
                out.write(", " + String.valueOf(new Date()));
//                out.write(" ,time:" + String.valueOf(this.data.getTime()));

                out.newLine();
                out.flush();
                out.close();
                fstream.close();

            } catch (IOException e) {
                e.printStackTrace();
            }


        }

//        else {
//            System.out.println("send complete.KKKKKKKKKKKKKKKKKKKKKKKKKKKKQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ");
//        }

    }


}

