import org.apache.kafka.common.protocol.types.Field;
import wangzitian.realtime.Transactions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class writeTransac
{
    public static void writeTransac2txt(Transactions data, long timestamp){
        String code = data.getSymbol().toString();
        String filepath = "./" + code + ".txt";
        File file = new File(filepath);
        try {
            if(!file.exists()){
            file.createNewFile();}
            FileWriter fw = new FileWriter(file, true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(code +"; ");
            bw.write(String.valueOf(data.getTradeTime()) +"; ");
            bw.write( String.valueOf(data.getBuyID()) + "; ");
            bw.write(String.valueOf(data.getSellID()) +"; ");
            bw.write( String.valueOf(data.getTradePrice())+"; ");
            bw.write( String.valueOf(data.getTradeVolume())+"; ");
            bw.write(String.valueOf(data.getTradeAmount())+"; ");
            Date time = new Date(timestamp);

            bw.write(String.valueOf(time) + "; ");
            Date now = new Date();
            bw.write(String.valueOf(now));

            bw.newLine();
            bw.flush();
            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }



    }

}
