
import java.io.IOException;
import java.text.ParseException;

public class run {
    public static void main(String args[]) throws IOException, ParseException {
        //JConsumerSubscribe jconsumer = new JConsumerSubscribe();
        JConsumer_hangqing jconsumer = new JConsumer_hangqing();
        jconsumer.start();
    }
}