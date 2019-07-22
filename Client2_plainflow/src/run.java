import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


import kafkaProc.TransacDeserilizer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import wangzitian.realtime.Transactions;

public class run {
//    public static void main(String[] args) throws IOException {
//        int threads_num = 8;
//        Codes.setThreadsNum(threads_num);
//        Codes.setSHlist("./SH_codes.txt");
//        Codes.setSZlist("./SZ_codes.txt");
//
//
//        System.out.println(Codes.SH_string);
//        System.out.println(Codes.SZ_string);
//
//        int total_num = Codes.code_talbe_names.size();
//        System.out.println("Total codes are:"+total_num);
//
//
////        MongoDB.createTables(); // Already created Annotated it
//
////        Kafka.buildProducers(); // create producers for different thread
////        MongoDB.buildMongoClients();//create dbpool for different thread
//        Kafka.buildSingleProducer();
//
//
//        IGTAQTSCallbackBase callback = new GTACallbackBase();
//        IGTAQTSApi baseService = GTAQTSApiBaseImpl.getInstance().CreateInstance(callback);
//        baseService.BaseInit();
//        baseService.BaseRegisterService("119.147.211.194", (short)8866);
//        baseService.BaseRegisterService("180.153.102.99", (short)8888);
//
//        baseService.BaseRegisterService("180.153.102.94", (short)8888);
//
//
//        int ret =  baseService.BaseLoginX("zlzb", "erSKS77Y", "NetType=0");
//        if (QTSDataType.RetCode.Ret_Success != QTSDataType.RetCode.fetchByCode(ret)) {
//            System.out.println("Login error:" + ret);
//            System.exit(-1);
//        }
//        else {
//            System.out.println("Success Login!");
//        }
//
//        ret = baseService.BaseSubscribe(MsgType.SSEL2_Transaction.code, Codes.SH_string);
//        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
//            System.out.println("Subscribe SH error!");
//            System.exit(-1);
//        }
//        else {
//            System.out.println("Subscribe SH Success!");
//        }
//
//        ret = baseService.BaseSubscribe(MsgType.SZSEL2_Transaction.code, Codes.SZ_string);
//        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
//            System.out.println("Subscribe SZ error!");
//            System.exit(-1);
//        }
//        else {
//            System.out.println("Subscribe SZ Success!");
//        }
//
//
//        Distributing dis0 = new Distributing();
//        Thread t0 = new Thread(dis0);
//        t0.start();
//
//        try {
//            Thread.sleep(1000 * 60 *20);
//            GLBuffer.working_flag = false;
//            t0.join();
//            Kafka.single_producer.close();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            System.exit(-5);
//        }
//
//
//        baseService.BaseUninit();
//        System.out.println("GTA API exit");
//    }

//    public static void main(String[] args) {
//        Transactions data = new Transactions();
//        data.setTradePrice(12.00);
//
//        System.out.println(data.getTradePrice());
//        System.out.println(data.getSymbol());
//    }


//    public static void main(String[] args) throws IOException {
//        Transactions transac1 = new Transactions();
//        transac1.setBuyID(19222222);
//        transac1.setSellID(15555555);
//        transac1.setTradePrice(2222.);
//        transac1.setTradeVolume(2111.);
//        transac1.setTradeAmount(transac1.getTradePrice()*transac1.getTradeVolume());
//        int threads_num = 8;
//        Codes.setThreadsNum(threads_num);
//        Codes.setSHlist("./SH_codes.txt");
//        Codes.setSZlist("./SZ_codes.txt");
//
//
//        System.out.println(Codes.SH_string);
//        System.out.println(Codes.SZ_string);
//        try {
//            Kafka.createTopic();
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println("kafka topic create failed");
//            System.exit(-1);
//        }
//    }
/*
    public static void main(String[] args) throws IOException,ParseException {
        int threads_num = 4;
        KafkaSend.buildSingleProducer();
        WeightOfCodes.setThreads_num(threads_num);
        WeightOfCodes.setSHweights("./SH_data.txt");
        WeightOfCodes.setSZweights("./SZ_data.txt");
        WeightOfCodes.getScheduledTickFromTXT("./schedulerTick_regular.txt");
        WeightOfCodes.getDelayTickFromTXT("./schedulerTick_regular.txt");

        for(int i=0;i<WeightOfCodes.topic4codes.size();i++){
            System.out.println("threadID:");

            System.out.println(WeightOfCodes.topic4codes.get(i));
        }
        System.out.println(WeightOfCodes.topic4codes);

        WeightOfCodes.setString_today();

        System.out.println(WeightOfCodes.string_today);
        Dist dist0 = new Dist();

//        ScheduledTask task0 = new ScheduledTask();
//        Thread t_scheduled = new Thread(task0);
//        t_scheduled.start();


         //build distributing threads_array
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092");
        props.put("group.id", "flow215");
        props.put("enable.auto.commit", "true");
//        props.put("auto.offset.reset", "earliest");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", TransacDeserilizer.class.getName());
        KafkaConsumer<String, Transactions> consumer =
                new KafkaConsumer<String, Transactions>(props);


        double expected_duration = 150;//minitues

        consumer.subscribe(WeightOfCodes.topic4codes);

        long start_time = System.currentTimeMillis();
        int thread_id = 0;
        double weight = 0;
        while (GLBuffer.working_flag) {
            System.out.println("I am in loop now");

            ConsumerRecords<String, Transactions> records = consumer.poll(100);
            System.out.println("A while looped");
            for (ConsumerRecord<String, Transactions> record:records) {
                System.out.print("new record arrive!");
                System.out.print(" offset:" + record.offset());
                System.out.print(" partition:" + record.partition());

                System.out.print(" Code in while:" + record.value().getSymbol());

                thread_id = WeightOfCodes.threadID4codes.get(record.value().getSymbol().toString());
                weight = WeightOfCodes.Weights.get(record.value().getSymbol().toString());
                transacProc procTask = new transacProc(record.value(), weight, thread_id, record.timestamp());
                dist0.threads.get(thread_id).execute(procTask);

            }

            long now = System.currentTimeMillis();

            double duration = Double.valueOf(now-start_time) / 1000. / 60; //have run how many time

            if(duration > expected_duration) {
                GLBuffer.working_flag = false;
            }
        }

//        try {
//            t_scheduled.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

    }

*/

        public static void main(String args[]) throws IOException, ParseException {

            Codes.setTopicName("./topicName.txt");
            int working_threadNum = 5;
            Codes.setThreads_num(working_threadNum);
            Codes.setSHweights("./SH_data.txt");
            Codes.setSZweights("./SZ_data.txt");
            System.out.println("Code Initial completed, total num:" + Codes.code_cnt +", " + Codes.Weights.size());
            for(Map.Entry entry:Codes.Weights.entrySet()) {
                System.out.println("Codes:" + entry.getKey() +"   Weight:"+entry.getValue() + "    Thread_:" + Codes.thread4codes.get(entry.getKey()));
            }

            SimpleDateFormat sdf = new SimpleDateFormat("MMddHHmmss");
            Date time_now = new Date();

            String now_str = sdf.format(time_now);


            KafkaSend.buildSingleProducer();
            TickUtils.getScheduledTickFromTXT("./schedulerTick.txt");
            TickUtils.getDelayTickFromTXT("./schedulerTick.txt");
            TickUtils.setString_today();
            System.out.println("Tick Setted, Total Tick num is:" + TickUtils.tick_num + "   , " + TickUtils.ScheduledTick.size() + ", "  + TickUtils.ScheduledTick.size());
            System.out.println("Today string used by format:" + TickUtils.string_today);
            for(int i=0;i<TickUtils.tick_num;i++) {
                System.out.println("Scheduled Tick:" + TickUtils.ScheduledTick.get(i));
                System.out.println("Lated Tick:" + TickUtils.LatedTick.get(i));
            }

            CodeResultsOfTick.initCodeResultsOfTick(TickUtils.ScheduledTick,Codes.Weights);




            Properties props = new Properties();
            props.put("bootstrap.servers", "192.168.1.101:9091,192.168.1.101:9092,192.168.1.101:9093");
            props.put("group.id", Codes.topicName + "_" + now_str);
            props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "latest");
        props.put("receive.buffer.bytes", 65536*15);
        props.put("socket.receive.buffer.bytes", 64*1024*10);
//        props.put("queued.max.message.chunks", 8);
//        props.put("num.consumer.fetchers", 4);
//            props.put("auto.offset.reset", "latest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", TransacDeserilizer.class.getName());
            KafkaConsumer<String, Transactions> consumer =
                    new KafkaConsumer<String, Transactions>(props);


            consumer.subscribe(Codes.topic4kafka); //订阅codes;

            long start_time = System.currentTimeMillis();
            double expected_duration = 500;

            int thread_id = 0;double weight = 0;

            ScheduledTask sch0 = new ScheduledTask();
            Thread t0 = new Thread(sch0);
            t0.start();

            while(GLBuffer.working_flag){
//                System.out.println("before poll");
                ConsumerRecords<String, Transactions> records =  consumer.poll(10);
                System.out.println("After Poll.");
                for(ConsumerRecord<String, Transactions> record:records) {
                    String code = record.value().getSymbol().toString();
                    thread_id = Codes.thread4codes.get(code);
                    weight = Codes.Weights.get(code);
                    GLBuffer.global_lock.lock();
                    transacProc procTask = new transacProc(record.value(), weight, thread_id, record.timestamp());
                    Codes.threads_array.get(thread_id).execute(procTask);
                    GLBuffer.global_lock.unlock();




                }

                long now = System.currentTimeMillis();

                double duration = Double.valueOf(now-start_time) / 1000. / 60; //have run how many time

                if(duration > expected_duration) {
                    GLBuffer.working_flag = false;
                }

            }
            try {
                for(int i=0;i<working_threadNum;i++){
                    Codes.threads_array.get(i).shutdown();
                }

                t0.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            KafkaSend.single_producer.close();
            consumer.close();





        }

}
