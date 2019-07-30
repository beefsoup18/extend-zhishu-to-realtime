
import com.gta.qts.c2j.adaptee.structure.BuySellLevelInfo3;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Index;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import wangzitian.realtime.ZhiShu;


public class ZhiShuHangQing_SZwriter implements Runnable{
    private SZSEL2_Index data;
    private int thread_id;
    private String code;
    private String code_tablename;

    public ZhiShuHangQing_SZwriter(SZSEL2_Index data, String code, String code_tablename, int thread_id){
        this.data =data;
        this.thread_id = thread_id;
        this.code = code;
        this.code_tablename = code_tablename + "_zhishu_hangqing";
    }

    @Override
    public void run() {
        int localTimeStamp = data.LocalTimeStamp;
        String quotationFlag = byteArr2String(data.QuotationFlag);
        Long Time = data.Time;
        String Symbol = byteArr2String(data.Symbol);
        double PreClosePrice = data.PreClosePrice;
        double OpenPrice = data.OpenPrice;
        double HighPrice = data.HighPrice;
        double LowPrice = data.LowPrice;
        double LastPrice = data.LastPrice;
        double TotalVolume = data.TotalVolume;

        String SymbolSource = byteArr2String(data.SymbolSource);
        String SecurityPhaseTag = byteArr2String(data.SecurityPhaseTag);
        long TotalNo = data.TotalNo;
        long SampleNo = data.SampleNo;

        ZhiShu data_ = new ZhiShu();

        data_.setLocalTimeStamp(localTimeStamp);
        data_.setQuotationFlag(quotationFlag);
        data_.setTime(Time);
        data_.setSymbol(Symbol);
        data_.setPreClosePrice(PreClosePrice);
        data_.setOpenPrice(OpenPrice);
        data_.setHighPrice(HighPrice);
        data_.setLowPrice(LowPrice);
        data_.setLastPrice(LastPrice);
        data_.setTotalVolumeSZ(TotalVolume);

        data_.setSymbolSourceSZ(SymbolSource);
        data_.setSecurityPhaseTagSZ(SecurityPhaseTag);
        data_.setTotalNoSZ(TotalNo);
        data_.setSampleNoSZ(SampleNo);


        ProducerRecord<String, ZhiShu> record = new ProducerRecord<String, ZhiShu>(this.code_tablename,data_);
        try{
//            Kafka.producers.get(this.thread_id).send(record, new ProducerCallback(transac, this.code_tablename));
//            Kafka.producers.get(this.thread_id).send(record).get();
            Kafka.zhishuhangqingOnly_producer.send(record, new ZhiShuHangQingProducerCallback());
//                Kafka.single_producer.send(record, new ProducerCallback(transac, this.code_tablename));
            System.out.println("sending");
//            Kafka.hangqingOnly_producer.send(record).get();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("code send to kafka failed" + this.code +", " + Time);
        }

    }

    public String byteArr2String(byte[] bytes){
        String results = null;
        try {
            results = new String(bytes,0, bytes.length, "UTF-8").trim();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return results;
    }
}
