
import java.io.IOException;
import java.text.ParseException;

public class run {
    public static void main(String args[]) throws IOException, ParseException {
        JConsumerSubscribe jconsumer = new JConsumerSubscribe();
        jconsumer.start();
    }
}