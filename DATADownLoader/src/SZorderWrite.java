import com.gta.qts.c2j.adaptee.structure.SZSEL2_Order;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Transaction;
import com.mongodb.client.MongoCollection;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import wangzitian.realtime.SZorder;
import wangzitian.realtime.Transactions;

import java.io.UnsupportedEncodingException;

public class SZorderWrite implements Runnable{
    private SZSEL2_Order data;
    private int thread_id;
    private String code;
    private String code_tablename;

    public SZorderWrite(SZSEL2_Order data, String code, String code_tablename, int thread_id){
        this.data =data;
        this.thread_id = thread_id;
        this.code = code;
        this.code_tablename = code_tablename + "_order";
    }

    @Override
    public void run() {

        int localTimeStamp = data.LocalTimeStamp;
        String quotationFlag = byteArr2String(data.QuotationFlag);
        int setID = data.SetID;
        long recID = data.RecID;
        String SymbolSource = byteArr2String(data.SymbolSource);
        long Time = data.Time;
        double OrderPrice = data.OrderPrice;
        double OrderVolume = data.OrderVolume;

        String OrderCode = byteArr2String(new byte[]{data.OrderCode});
        String OrderType = byteArr2String(new byte[]{data.OrderType});

        SZorder orderdata = new SZorder();
        orderdata.setLocalTimeStamp(localTimeStamp);
        orderdata.setQuotationFlag(quotationFlag);
        orderdata.setSetID(setID);
        orderdata.setRecID(recID);
        orderdata.setSymbol(this.code);
        orderdata.setSymbolSource(SymbolSource);
        orderdata.setTime(Time);
        orderdata.setOrderPrice(OrderPrice);
        orderdata.setOrderVolume(OrderVolume);
        orderdata.setOrderCode(OrderCode);
        orderdata.setOrderType(OrderType);



        ProducerRecord<String, SZorder> record = new ProducerRecord<String, SZorder>(this.code_tablename,orderdata);
        try{
//            Kafka.producers.get(this.thread_id).send(record).get();
//                Kafka.single_producer.send(record, new ProducerCallback(transac, this.code_tablename));
//            Kafka.OrderProducers.get(this.thread_id).send(record, new OrderPrducerCallback(orderdata, this.code_tablename));
            Kafka.orderOnly_producer.send(record, new OrderPrducerCallback());
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("code order send to kafka failed" + this.code +", " + Time);
        }





//        MongoCollection<Document> collection = MongoDB.dbPool.get(this.thread_id).getCollection(this.code_tablename);
//        Document doc = new Document("code", code)
//                .append("buyorderid", buyID)
//                .append("sellorderid", sellID)
//                .append("tradeprice", price)
//                .append("tradevolume",volume)
//                .append("tradetime", tradeTime)
//                .append("trademoney", tradeAmount) //the end of the common
//
//                .append("LocalTimeStamp", localTimeStamp)
//                .append("QuotationFlag", quotationFlag)
//                .append("SetID", setID)
//                .append("RecID", recID)
//                .append("SymbolSource", SymbolSource)
//                .append("TradeType", TradeType);
//
//        collection.insertOne(doc);

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
