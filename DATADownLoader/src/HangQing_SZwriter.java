
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Quotation;

import com.gta.qts.c2j.adaptee.structure.SZSE_BuySellLevelInfo3;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import wangzitian.realtime.HangQing;


public class HangQing_SZwriter implements Runnable{
    private SZSEL2_Quotation data;
    private int thread_id;
    private String code;
    private String code_tablename;

    public HangQing_SZwriter(SZSEL2_Quotation data, String code, String code_tablename, int thread_id){
        this.data =data;
        this.thread_id = thread_id;
        this.code = code;
        this.code_tablename = code_tablename + "_hangqing";
    }

    @Override
    public void run() {
        /**把各个属性从qts原本的数据对象中提取出来，然后利用提取的数值设置avro对象**/

        int localTimeStamp = data.LocalTimeStamp;
        String quotationFlag = byteArr2String(data.QuotationFlag);

        Long Time = data.Time;

        String SymbolSource = byteArr2String(data.SymbolSource);

        double PreClosePrice = data.PreClosePrice;
        double OpenPrice = data.OpenPrice;
        double HighPrice = data.HighPrice;
        double LowPrice = data.LowPrice;
        double LastPrice = data.LastPrice;
        double ClosePrice = data.ClosePrice;

        double PriceUpLimit = data.PriceUpLimit;
        double PriceDownLimit = data.PriceDownLimit;
        double PriceUpdown1 = data.PriceUpdown1;
        double PriceUpdown2 = data.PriceUpdown2;
        Long TotalNo = data.TotalNo;
        double TotalVolume = data.TotalVolume;
        double TotalAmout = data.TotalAmount;
        String SecurityPhaseTag = byteArr2String(data.SecurityPhaseTag);
        double PERatio1 = data.PERatio1;
        double NAV = data.NAV;
        double PERatio2 = data.PERatio2;
        double IOPV = data.IOPV;
        double PremiumRate = data.PremiumRate;
        double TotalSellOrderVolume = data.TotalSellOrderVolume;

        double WtAvgSellPrice = data.WtAvgSellPrice;
        int SellLevelNo = data.SellLevelNo;
        int SellLevelQueueNo01 = data.SellLevelQueueNo01;
        double[] SellLevelQueue = data.SellLevelQueue;

        double TotalBuyOrderVolume = data.TotalBuyOrderVolume;

        double WtAvgBuyPrice = data.WtAvgBuyPrice;

        int BUyLevelNo = data.BuyLevelNo;
        int BuyLevelQueueNo01 = data.BuyLevelQueueNo01;
        double[] BuyLevelQueue = data.BuyLevelQueue;

        double WtAvgRate = data.WtAvgRate;
        double WtAvgRateUpdown = data.WtAvgRateUpdown;
        double PreWtAvgRate = data.PreWtAvgRate;

        HangQing data_ = new HangQing();

        data_.setLocalTimeStamp(localTimeStamp);
        data_.setQuotationFlag(quotationFlag);

        data_.setTime(Time);
        data_.setSymbol(this.code);
        data_.setPreClosePrice(PreClosePrice);
        data_.setOpenPrice(OpenPrice);
        data_.setHighPrice(HighPrice);
        data_.setLowPrice(LowPrice);
        data_.setLastPrice(LastPrice);

        data_.setPriceUpLimitSZ(PriceUpLimit);
        data_.setPriceDownLimitSZ(PriceDownLimit);
        data_.setPriceUpdown1SZ(PriceUpdown1);
        data_.setPriceUpdown2SZ(PriceUpdown2);
        data_.setTotalNo(TotalNo);
        data_.setTotalVolume(TotalVolume);
        data_.setTotalAmount(TotalAmout);

        data_.setClosePrice(ClosePrice);


        data_.setSecurityPhaseTag(SecurityPhaseTag);

        data_.setPERatio1SZ(PERatio1);
        data_.setNAVSZ(NAV);
        data_.setPERatio2SZ(PERatio2);

        data_.setIOPV(IOPV);
        data_.setPRemiumRateSZ(PremiumRate);
        data_.setTotalSellOrderVolume(TotalSellOrderVolume);
        data_.setWtAvgSellPrice(WtAvgSellPrice);
        data_.setSellLevelNo(SellLevelNo);

        data_.setSellLevelQueueNo01(SellLevelQueueNo01);

        ArrayList<Double> SellLevelQueueArray = new ArrayList<>();
        for (int i=0;i<SellLevelQueue.length;i++)
        {
            SellLevelQueueArray.add((double)SellLevelQueue[i]);
        }
        data_.setSellLevelQueue(SellLevelQueueArray);

        data_.setTotalBuyOrderVolume(TotalBuyOrderVolume);
        data_.setWtAvgBuyPrice(WtAvgBuyPrice);

        data_.setBuyLevelNo(BUyLevelNo);

        data_.setBuyLevelQueueNo01(BuyLevelQueueNo01);

        ArrayList<Double> BuyLevelQueueArray = new ArrayList<>();
        for (int i=0;i<SellLevelQueue.length;i++)
        {
            BuyLevelQueueArray.add((double)BuyLevelQueue[i]);
        }
        data_.setBuyLevelQueue(BuyLevelQueueArray);

        data_.setWtAvgRateSZ(WtAvgRate);
        data_.setWtAvgRateUpdownSZ(WtAvgRateUpdown);
        data_.setPreWtAvgRateSZ(PreWtAvgRate);

        ArrayList<Double> sellLevel_price = new ArrayList<>();
        ArrayList<Long> sellLevel_TotalOrderNo = new ArrayList<>();
        ArrayList<Double> sellLevel_Volume = new ArrayList<>();

        SZSE_BuySellLevelInfo3[] SellLevel = data.SellLevel;
        for(int i=0;i<SellLevel.length;i++){
            sellLevel_price.add(SellLevel[i].Price);
            sellLevel_TotalOrderNo.add(SellLevel[i].TotalOrderNo);
            sellLevel_Volume.add(SellLevel[i].Volume);
        }

        ArrayList<Double> buyLevel_price = new ArrayList<>();
        ArrayList<Long> buyLevel_TotalOrderNo = new ArrayList<>();
        ArrayList<Double> buyLevel_Volume = new ArrayList<>();

        SZSE_BuySellLevelInfo3[] BuyLevel = data.BuyLevel;
        for(int i=0;i<SellLevel.length;i++){
            buyLevel_price.add(BuyLevel[i].Price);
            buyLevel_TotalOrderNo.add(BuyLevel[i].TotalOrderNo);
            buyLevel_Volume.add(BuyLevel[i].Volume);
        }

        data_.setBuyLevelVolume(buyLevel_Volume);
        data_.setBuyLevelTotalOrderNo(buyLevel_TotalOrderNo);
        data_.setBuyLevelPrice(buyLevel_price);
        data_.setSellLevelVolume(sellLevel_Volume);
        data_.setSellLevelTotalOrderNo(sellLevel_TotalOrderNo);
        data_.setSellLevelPrice(sellLevel_price);

        //------


//
//        data_.setBondWtAvgBuyPrice(BondWtAvgBuyPrice);
//
//
//        data_.setBongWtAvgSellPrice(BondWtAvgSellPrice);
//
//        data_.setETFBuyNo(ETFBuyNo);
//        data_.setETFBuyVolume(ETFBuyVolume);
//        data_.setETFBuyAmount(ETFBuyAmount);
//
//        data_.setETFSellNo(ETFSellNo);
//        data_.setETFSellVolume(ETFSellVolume);
//        data_.setETFSellAmount(ETFSellAmount);
//        data_.setYTM(YTM);
//        data_.setTotalWarrantExecVol(TotalWarrantExecVol);
//        data_.setWarrantDownLimit(WarrantDownLimit);
//        data_.setWarrantUpLimit(WarrantUpLimit);
//        data_.setWithdrawBuyNo(WithdrawBuyNo);
//        data_.setWithdrawBuyVolume(WithdrawBuyVolume);
//        data_.setWithdrawBuyAmount(WithdrawBuyAmount);
//        data_.setWithdrawSellNo(WithdrawSellNo);
//        data_.setWithdrawSellVolume(WithdrawSellVolume);
//        data_.setWithdrawSellAmount(WithdrawSellAmount);
//        data_.setTotalBuyNo(TotalBuyNo);
//        data_.setTotalSellNo(TotalSellNo);
//        data_.setMaxBuyDuration(MaxBuyDuration);
//        data_.setMaxSellDuration(MaxSellDuration);
//        data_.setBuyOrderNo(BuyOrderNo);
//        data_.setSellOrderNo(SellOrderNo);
//
//
//
//        ArrayList<BuySellLevelIn3avro> sellLevelarray = new ArrayList<>();
//        for(int i=0;i<SellLevel.length;i++){
//            BuySellLevelIn3avro avrodata = new BuySellLevelIn3avro();
//            avrodata.setPrice(SellLevel[i].Price);
//            avrodata.setTotalOrderNo(SellLevel[i].TotalOrderNo);
//            avrodata.setVolume(SellLevel[i].Volume);
//
//            sellLevelarray.add(avrodata);
//        }
//        data_.setSellLevel(sellLevelarray);

//        ArrayList<BuySellLevelIn3avro> buyLevelarray = new ArrayList<>();
//        for(int i=0;i<BuyLevel.length;i++){
//            BuySellLevelIn3avro avrodata = new BuySellLevelIn3avro();
//            avrodata.setPrice(BuyLevel[i].Price);
//            avrodata.setTotalOrderNo(BuyLevel[i].TotalOrderNo);
//            avrodata.setVolume(BuyLevel[i].Volume);
//
//            buyLevelarray.add(avrodata);
//        }
//        data_.setBuyLevel(buyLevelarray);


        ProducerRecord<String, HangQing> record = new ProducerRecord<String, HangQing>(this.code_tablename, data_);
        try{
//            Kafka.producers.get(this.thread_id).send(record, new ProducerCallback(transac, this.code_tablename));
//            Kafka.producers.get(this.thread_id).send(record).get();
            Kafka.hangqingOnly_producer.send(record, new HqPrducerCallback());
//            System.out.println("sending");
//            Kafka.hangqingOnly_producer.send(record).get();
//                Kafka.single_producer.send(record, new ProducerCallback(transac, this.code_tablename));
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
