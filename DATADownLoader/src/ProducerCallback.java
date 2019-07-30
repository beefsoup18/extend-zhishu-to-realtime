
/**成交数据的Kafka生产者**/

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import wangzitian.realtime.Transactions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;


public class ProducerCallback implements Callback{

    public ProducerCallback(){

    }
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception){
        if(exception != null) {
            exception.printStackTrace();
                    try {
                        FileWriter fstream = new FileWriter("./callbackException.txt", true);
                        BufferedWriter out = new BufferedWriter(fstream);
                        out.write(exception.toString());
                        out.write(", " + String.valueOf(new Date()));
//                        out.write(" ,tradetime:" + String.valueOf(this.data.getTradeTime()));
//                        out.write(" ,sellID:"  + String.valueOf(this.data.getSellID()));
//                        out.write(" ,buyID:" + String.valueOf(this.data.getBuyID()));

                        out.newLine();
                        out.flush();
                        out.close();
                        fstream.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                }

//        else {
//            System.out.println("send complete.-------------------LOL-------------------------LOL-------------------");
//        }

    }


}
