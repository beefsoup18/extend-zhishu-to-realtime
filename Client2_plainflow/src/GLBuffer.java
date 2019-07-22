import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GLBuffer {
    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> IN =
            new ConcurrentHashMap<String, ConcurrentHashMap<String,Double>>();

    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> OUT =
            new ConcurrentHashMap<String, ConcurrentHashMap<String,Double>>();

    public static boolean working_flag = true;
    public static Lock global_lock = new ReentrantLock();
    public static long remote_clock = 0;
}
