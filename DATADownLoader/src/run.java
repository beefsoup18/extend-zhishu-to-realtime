import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.gta.qts.c2j.adaptee.IGTAQTSApi;
import com.gta.qts.c2j.adaptee.IGTAQTSCallbackBase;
import com.gta.qts.c2j.adaptee.impl.GTAQTSApiBaseImpl;
import com.gta.qts.c2j.adaptee.structure.QTSDataType;

import com.gta.qts.c2j.adaptee.structure.QTSDataType.MsgType;
import com.gta.qts.c2j.adaptee.structure.QTSDataType.RetCode;
import kafkaProc.TransacDeserilizer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import wangzitian.realtime.Transactions;


public class run {
    public static void main(String[] args) throws IOException {
        int threads_num = 10;
        Codes.setThreadsNum(threads_num);
        Codes.setSHlist("C:\\Users\\beefsoup\\Documents\\extend-zhishu-to-realtime\\DATADownLoader\\SH_codes.txt");
        Codes.setSZlist("C:\\Users\\beefsoup\\Documents\\extend-zhishu-to-realtime\\DATADownLoader\\SZ_codes.txt");
        Codes.setzhishuSZlist("C:\\Users\\beefsoup\\Documents\\extend-zhishu-to-realtime\\DATADownLoader\\zhishu_SZ_codes.txt");
        Codes.setzhishuSHlist("C:\\Users\\beefsoup\\Documents\\extend-zhishu-to-realtime\\DATADownLoader\\zhishu_SH_codes.txt");
//        Codes.setSHlist("./SH_codes.txt");
//        Codes.setSZlist("./SZ_codes.txt");


        System.out.println(Codes.SH_string);
        System.out.println(Codes.SZ_string);
        System.out.println(Codes.zhishu_SH_string);
        System.out.println(Codes.zhishu_SZ_string);
//        System.exit(-5);

        int total_num = Codes.code_talbe_names.size();
        System.out.println("Total codes are:"+total_num);

        //构建Kafka生产者，其中实现了序列化
        Kafka.buildSingleProducer();
        System.out.println("创建成交数据生产者");
        Kafka.buildOrderOnlyProducers();
        System.out.println("创建委托数据生产者");
        Kafka.buildHangQingOnlyProducers();
        System.out.println("创建行情数据生产者");
        Kafka.buildZhiShuHangQingOnlyProducers();
        System.out.println("创建指数数据生产者");

        System.out.println("创建线程池");
        Codes.buildGLThreadsPool();

        IGTAQTSCallbackBase callback = new GTACallbackBase();
        //创建从QTA实时取数据并发送到Kafka集群的服务
        IGTAQTSApi baseService = GTAQTSApiBaseImpl.getInstance().CreateInstance(callback);
        baseService.BaseInit();
        // 4个行情数据接口，自动从能连通的接口取数据
        System.out.println("连接4个数据接口");
        baseService.BaseRegisterService("119.147.211.219", (short)8866);
        baseService.BaseRegisterService("119.147.211.220", (short)8866);
        baseService.BaseRegisterService("180.153.102.99", (short)8888);
        baseService.BaseRegisterService("180.153.102.94", (short)8888);

        //登录
//        int ret =  baseService.BaseLoginX("bjzskj", "DzqbCjM9", "NetType=0");  //正式账号
        int ret =  baseService.BaseLoginX("bjzskj_test", "ry5RU5x4", "NetType=0");  //测试账号
        if (QTSDataType.RetCode.Ret_Success != QTSDataType.RetCode.fetchByCode(ret)) {
            System.out.println("Login error:" + ret);
            System.exit(-1);
        }
        else {
            System.out.println("Success Login!");
        }

        /**
         接下来订阅数据：
         1.上交所逐笔成交数据
         2.深交所逐笔成交数据
         3.上交所实时行情
         4.深交所实时行情
         5.深交所逐笔委托
         **/
        //订阅上交所逐笔成交数据
        ret = baseService.BaseSubscribe(MsgType.SSEL2_Transaction.code, Codes.SH_string);
        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
            System.out.println("Subscribe SH error!");
            System.exit(-1);
        }
        else {
            System.out.println("Subscribe SH Success!");
        }

        //订阅深交所逐笔成交数据
        ret = baseService.BaseSubscribe(MsgType.SZSEL2_Transaction.code, Codes.SZ_string);
        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
            System.out.println("Subscribe SZ error!");
            System.exit(-1);
        }
        else {
            System.out.println("Subscribe SZ Success!");
        }

        //订阅上交所实时行情
        ret = baseService.BaseSubscribe(MsgType.SSEL2_Quotation.code, Codes.SH_string);
        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
            System.out.println("Subscribe SH hangqing error!");
            System.exit(-1);
        }
        else {
            System.out.println("Subscribe SH hangqing Success!");
        }

        //订阅上交所指数行情
        ret = baseService.BaseSubscribe(MsgType.SSEL2_Index.code, Codes.zhishu_SH_string);
        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
            System.out.println("Subscribe SH zhishu error!");
            System.exit(-1);
        }
        else {
            System.out.println("Subscribe SH zhishuhangqing Success!");
        }

        //订阅深交所实时行情
        ret = baseService.BaseSubscribe(MsgType.SZSEL2_Quotation.code, Codes.SZ_string);
        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
            System.out.println("Subscribe SZ hangqing error!");
            System.exit(-1);
        }
        else {
            System.out.println("Subscribe SZ hangqing Success!");
        }

        //订阅深交所逐笔委托
        ret = baseService.BaseSubscribe(MsgType.SZSEL2_Order.code, Codes.SZ_string);
        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
            System.out.println("Subscribe SZ Order error!");
            System.exit(-1);
        }
        else {
            System.out.println("Subscribe SZ Order Success!");
        }

        //订阅深交所指数行情 param: 消息结构体、Codes.SZ_string
        ret = baseService.BaseSubscribe(MsgType.SZSEL2_Index.code, Codes.zhishu_SZ_string);
        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
            System.out.println("Subscribe SZ zhishu hangqing error!");
            System.exit(-1);
        }
        else {
            System.out.println("Subscribe SZ zhishu hangqing Success!");
        }


        /**建立3个线程分别处理数据**/
        Distributing dis0 = new Distributing();
        OrderDistributor dis1 = new OrderDistributor();
        HangQingDistributor dis2 = new HangQingDistributor();
        ZhiShuHangQingDistributor dis3 = new ZhiShuHangQingDistributor();
        Thread t0 = new Thread(dis0);
        Thread t1 = new Thread(dis1);
        Thread t2 = new Thread(dis2);
        Thread t3 = new Thread(dis3);
        t0.start();
        t1.start();
        t2.start();
        t3.start();

        try {
            Thread.sleep(1000 * 60 * 500);
            GLBuffer.working_flag = false;
            t0.join();
            t1.join();
            t2.join();
            t3.join();
            Codes.closeGLThreadPool();
            Kafka.single_producer.close();
            Kafka.hangqingOnly_producer.close();
            Kafka.orderOnly_producer.close();
            Kafka.zhishuhangqingOnly_producer.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-5);
        }

        baseService.BaseUninit();
        System.out.println("GTA API exit");

    }
}
