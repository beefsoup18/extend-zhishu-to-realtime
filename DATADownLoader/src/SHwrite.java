
import com.gta.qts.c2j.adaptee.structure.SSEL2_Transaction;
import com.mongodb.client.MongoCollection;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import wangzitian.realtime.Transactions;

import java.io.UnsupportedEncodingException;

public class SHwrite implements Runnable{
    private SSEL2_Transaction data;
    private int thread_id;
    private String code;
    private String code_tablename;

    public SHwrite(SSEL2_Transaction data, String code, String code_tablename, int thread_id){
        this.data =data;
        this.thread_id = thread_id;
        this.code = code;
        this.code_tablename = code_tablename;
    }

    @Override
    public void run() {
        long buyID = data.BuyRecID;
        long sellID = data.SellRecID;
        double price = data.TradePrice;
        double volume = data.TradeVolume;
        double tradeAmount = data.TradeAmount;
        long tradeTime = data.TradeTime;//important fields

        int localTimeStamp = data.LocalTimeStamp;
        String quotationFlag = byteArr2String(data.QuotationFlag);
        long PacketTimeStamp = data.PacketTimeStamp;
        long recID = data.RecID;
        int TradeChannel = data.TradeChannel;
        String BuySellFlag = byteArr2String(new byte[]{data.BuySellFlag});


        Transactions transac = new Transactions();
        transac.setSymbol(this.code);
        transac.setBuyID(buyID);
        transac.setSellID(sellID);
        transac.setTradePrice(price);
        transac.setTradeVolume(volume);
        transac.setTradeAmount(tradeAmount);
        transac.setTradeTime(tradeTime);
        transac.setLocalTimeStamp(localTimeStamp);
        transac.setQuotationFlag(quotationFlag);
        transac.setPacketTimeStampSH(PacketTimeStamp);
        transac.setRecID(recID);
        transac.setTradeChannelSH(TradeChannel);
        transac.setBuySellFlagSH(BuySellFlag);

        ProducerRecord<String, Transactions> record = new ProducerRecord<String, Transactions>(this.code_tablename,transac);
        try{
//            Kafka.producers.get(this.thread_id).send(record, new ProducerCallback(transac, this.code_tablename));
//            Kafka.producers.get(this.thread_id).send(record).get();
                Kafka.single_producer.send(record, new ProducerCallback());
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("code send to kafka failed" + this.code +", " + tradeTime);
        }




/*


        MongoCollection<Document> collection = MongoDB.dbPool.get(this.thread_id).getCollection(this.code_tablename);
        Document doc = new Document("code", code)
                .append("buyorderid", buyID)
                .append("sellorderid", sellID)
                .append("tradeprice", price)
                .append("tradevolume",volume)
                .append("tradetime", tradeTime)
                .append("trademoney", tradeAmount) //the end of the common

                .append("LocalTimeStamp", localTimeStamp)
                .append("QuotationFlag", quotationFlag)
                .append("PacketTimeStamp", PacketTimeStamp)
                .append("RecID", recID)
                .append("TradeChannel", TradeChannel)
                .append("BuySellFlag", BuySellFlag);

        collection.insertOne(doc);
        */

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
