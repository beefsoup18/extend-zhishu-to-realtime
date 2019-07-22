
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import wangzitian.realtime.SZorder;
import wangzitian.realtime.Transactions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;


public class OrderPrducerCallback implements Callback{


    public OrderPrducerCallback(){

    }
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception){
        if(exception != null) {
            exception.printStackTrace();
            try {
                FileWriter fstream = new FileWriter("./callbackException_order.txt", true);
                BufferedWriter out = new BufferedWriter(fstream);
                out.write(exception.toString());
                out.write(", " + String.valueOf(new Date()));
//                out.write(" ,ordertime:" + String.valueOf(this.data.getTime()));
//                out.write(" ,orderPrice:"  + String.valueOf(this.data.getOrderPrice()));
//                out.write(" ,orderVolume:" + String.valueOf(this.data.getOrderVolume()));

                out.newLine();
                out.flush();
                out.close();
                fstream.close();

            } catch (IOException e) {
                e.printStackTrace();
            }


        }

        else {
            System.out.println("send complete.---HAHAHAHAHAHAHAHAHAHAHAHA");
        }

    }


}
