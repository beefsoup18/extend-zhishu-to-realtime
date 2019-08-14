
/**指数行情的Kafka生产者**/

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class ZhiShuHangQingProducerCallback implements Callback{

    public ZhiShuHangQingProducerCallback(){

    }
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception){
        if(exception != null) {
            exception.printStackTrace();
            try {
                FileWriter fstream = new FileWriter("./callbackException_zhishuhangqing.txt", true);
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
            System.out.println("send failed.!!!!!!!!!!! zhishu");
        }

        else {
            System.out.println("send complete.-----------LOL------------------");
        }

    }


}

