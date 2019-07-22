import java.text.SimpleDateFormat;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Order;
public class GLBuffer {
    public static Queue<transacData> databuffer = new LinkedBlockingQueue<transacData>();  //...逐笔成交数据（深市+沪市）
    public static Queue<SZSEL2_Order> orderBuffer = new LinkedBlockingQueue<SZSEL2_Order>();  //...逐笔委托数据（深市）
    public static Queue<HangQingHolder> hangqingBuffer = new LinkedBlockingQueue<HangQingHolder>();  //...逐笔行情数据（深市+沪市）
    public static Queue<IndexHangQingHolder> zhishuhangqingbuffer = new LinkedBlockingQueue<ZhiShuHangQingHolder>();  //指数行情数据（深市+沪市）
    public static boolean working_flag = true;
    public static SimpleDateFormat df_day = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat df_time = new SimpleDateFormat("yy-MM-dd,HH:mm:ss");
}
