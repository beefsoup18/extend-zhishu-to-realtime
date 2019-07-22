
import com.gta.qts.c2j.adaptee.structure.BuySellLevelInfo3;
import com.gta.qts.c2j.adaptee.structure.SSEL2_Quotation;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import wangzitian.realtime.HangQing;


public class HangQing_SHwriter implements Runnable{
    private SSEL2_Quotation data;
    private int thread_id;
    private String code;
    private String code_tablename;

    public HangQing_SHwriter(SSEL2_Quotation data, String code, String code_tablename, int thread_id){
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
        long PacketTimeStamp = data.PacketTimeStamp;
        int Time = data.Time;

        double PreClosePrice = data.PreClosePrice;
        double OpenPrice = data.OpenPrice;
        double HighPrice = data.HighPrice;
        double LowPrice = data.LowPrice;
        double LastPrice = data.LastPrice;
        double ClosePrice = data.ClosePrice;
        String TradeStatus = byteArr2String(data.TradeStatus);
        String SecurityPhaseTag = byteArr2String(data.SecurityPhaseTag);
        Long TotalNo = data.TotalNo;
        Long TotalVolume = data.TotalVolume;
        double TotalAmout = data.TotalAmount;
        Long TotalBuyOrderVolume = data.TotalBuyOrderVolume;
        double WtAvgBuyPrice = data.WtAvgBuyPrice;
        double BondWtAvgBuyPrice = data.BondWtAvgBuyPrice;
        Long TotalSellOrderVolume = data.TotalSellOrderVolume;
        double WtAvgSellPrice = data.WtAvgSellPrice;
        double BondWtAvgSellPrice = data.BondWtAvgSellPrice;
        double IOPV = data.IOPV;
        int ETFBuyNo = data.ETFBuyNo;
        Long ETFBuyVolume = data.ETFBuyVolume;
        double ETFBuyAmount = data.ETFBuyAmount;
        int ETFSellNo = data.ETFSellNo;
        Long ETFSellVolume = data.ETFSellVolume;
        double ETFSellAmount = data.ETFSellAmount;
        double YTM = data.YTM;
        Long TotalWarrantExecVol = data.TotalWarrantExecVol;
        double WarrantDownLimit = data.WarrantDownLimit;
        double WarrantUpLimit = data.WarrantUpLimit;
        int WithdrawBuyNo = data.WithdrawBuyNo;
        Long WithdrawBuyVolume = data.WithdrawBuyVolume;
        double WithdrawBuyAmount = data.WithdrawBuyAmount;
        int WithdrawSellNo = data.WithdrawSellNo;
        Long WithdrawSellVolume = data.WithdrawSellVolume;
        double WithdrawSellAmount = data.WithdrawSellAmount;

        int TotalBuyNo = data.TotalBuyNo;
        int TotalSellNo = data.TotalSellNo;
        int MaxBuyDuration = data.MaxBuyDuration;
        int MaxSellDuration = data.MaxSellDuration;
        int BuyOrderNo = data.BuyOrderNo;
        int SellOrderNo = data.SellOrderNo;

        int SellLevelNo = data.SellLevelNo;

        BuySellLevelInfo3 [] SellLevel = data.SellLevel;

        int SellLevelQueueNo01 = data.SellLevelQueueNo01;
        int[] SellLevelQueue = data.SellLevelQueue;

        int BUyLevelNo = data.BuyLevelNo;
        BuySellLevelInfo3[] BuyLevel = data.BuyLevel;
        int BuyLevelQueueNo01 = data.BuyLevelQueueNo01;
        int[] BuyLevelQueue = data.BuyLevelQueue;


        HangQing data_ = new HangQing();

        data_.setLocalTimeStamp(localTimeStamp);
        data_.setQuotationFlag(quotationFlag);
        data_.setPacketTimeStampSH(PacketTimeStamp);
        data_.setTime(new Long(Time));
        data_.setSymbol(this.code);
        data_.setPreClosePrice(PreClosePrice);
        data_.setOpenPrice(OpenPrice);
        data_.setHighPrice(HighPrice);
        data_.setLowPrice(LowPrice);
        data_.setLastPrice(LastPrice);
        data_.setClosePrice(ClosePrice);
        data_.setTradeStatus(TradeStatus);
        data_.setSecurityPhaseTag(SecurityPhaseTag);
        data_.setTotalNo(TotalNo);
        data_.setTotalVolume(TotalVolume.doubleValue());
        data_.setTotalAmount(TotalAmout);
        data_.setTotalBuyOrderVolume(TotalBuyOrderVolume.doubleValue());
        data_.setWtAvgBuyPrice(WtAvgBuyPrice);
        data_.setBondWtAvgBuyPrice(BondWtAvgBuyPrice);
        data_.setTotalSellOrderVolume(TotalSellOrderVolume.doubleValue());
        data_.setWtAvgSellPrice(WtAvgSellPrice);
        data_.setBongWtAvgSellPrice(BondWtAvgSellPrice);
        data_.setIOPV(IOPV);
        data_.setETFBuyNo(ETFBuyNo);
        data_.setETFBuyVolume(ETFBuyVolume);
        data_.setETFBuyAmount(ETFBuyAmount);

        data_.setETFSellNo(ETFSellNo);
        data_.setETFSellVolume(ETFSellVolume);
        data_.setETFSellAmount(ETFSellAmount);
        data_.setYTM(YTM);
        data_.setTotalWarrantExecVol(TotalWarrantExecVol);
        data_.setWarrantDownLimit(WarrantDownLimit);
        data_.setWarrantUpLimit(WarrantUpLimit);
        data_.setWithdrawBuyNo(WithdrawBuyNo);
        data_.setWithdrawBuyVolume(WithdrawBuyVolume);
        data_.setWithdrawBuyAmount(WithdrawBuyAmount);
        data_.setWithdrawSellNo(WithdrawSellNo);
        data_.setWithdrawSellVolume(WithdrawSellVolume);
        data_.setWithdrawSellAmount(WithdrawSellAmount);
        data_.setTotalBuyNo(TotalBuyNo);
        data_.setTotalSellNo(TotalSellNo);
        data_.setMaxBuyDuration(MaxBuyDuration);
        data_.setMaxSellDuration(MaxSellDuration);
        data_.setBuyOrderNo(BuyOrderNo);
        data_.setSellOrderNo(SellOrderNo);

        data_.setSellLevelNo(SellLevelNo);

        ArrayList<Double> sellLevel_price = new ArrayList<>();
        ArrayList<Long> sellLevel_TotalOrderNo = new ArrayList<>();
        ArrayList<Double> sellLevel_Volume = new ArrayList<>();
        for(int i=0;i<SellLevel.length;i++){

            sellLevel_price.add(SellLevel[i].Price);
            sellLevel_TotalOrderNo.add(new Long(SellLevel[i].TotalOrderNo));
            sellLevel_Volume.add(new Long(SellLevel[i].Volume).doubleValue());



        }
        data_.setSellLevelPrice(sellLevel_price);
        data_.setSellLevelTotalOrderNo(sellLevel_TotalOrderNo);
        data_.setSellLevelVolume(sellLevel_Volume);

        data_.setSellLevelQueueNo01(SellLevelQueueNo01);

        ArrayList<Double> SellLevelQueueArray = new ArrayList<>();
        for (int i=0;i<SellLevelQueue.length;i++)
        {
            SellLevelQueueArray.add((double)SellLevelQueue[i]);
        }
        data_.setSellLevelQueue(SellLevelQueueArray);

        data_.setBuyLevelNo(BUyLevelNo);

        ArrayList<Double> buyLevel_price = new ArrayList<>();
        ArrayList<Long> buyLevel_TotalOrderNo = new ArrayList<>();
        ArrayList<Double> buyLevel_Volume = new ArrayList<>();
        for(int i=0;i<BuyLevel.length;i++){
            buyLevel_price.add(BuyLevel[i].Price);
            buyLevel_TotalOrderNo.add(new Long(BuyLevel[i].TotalOrderNo));
            buyLevel_Volume.add(new Long(BuyLevel[i].Volume).doubleValue());
        }
        data_.setBuyLevelPrice(buyLevel_price);
        data_.setBuyLevelTotalOrderNo(buyLevel_TotalOrderNo);
        data_.setBuyLevelVolume(buyLevel_Volume);

        ArrayList<Double> BuyLevelQueueArray = new ArrayList<>();
        for (int i=0;i<SellLevelQueue.length;i++)
        {
            BuyLevelQueueArray.add((double)BuyLevelQueue[i]);
        }
        data_.setBuyLevelQueue(BuyLevelQueueArray);

//        data_.setBuyLevelQueue(Arrays.asList(BuyLevelQueue));
        data_.setBuyLevelQueueNo01(BuyLevelQueueNo01);




        ProducerRecord<String, HangQing> record = new ProducerRecord<String, HangQing>(this.code_tablename,data_);
        try{
//            Kafka.producers.get(this.thread_id).send(record, new ProducerCallback(transac, this.code_tablename));
//            Kafka.producers.get(this.thread_id).send(record).get();
            Kafka.hangqingOnly_producer.send(record, new HqPrducerCallback());
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
