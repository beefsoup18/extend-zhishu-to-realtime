
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class CalcRangeSum {
    public static double calc_sum_low(ConcurrentHashMap<String, Double> flow_map) {

        double results = flow_map.values().stream().filter(p->p<=100000).mapToDouble(f->f.doubleValue()).sum();

        return results;
    }


    public static double calc_sum_middle(ConcurrentHashMap<String, Double> flow_map) {

        double results = flow_map.values().stream().filter(p->p>100000&&p<=250000).mapToDouble(f->f.doubleValue()).sum();

        return results;
    }


    public static double calc_sum_big(ConcurrentHashMap<String, Double> flow_map) {

        double results = flow_map.values().stream().filter(p->p>250000&&p<=500000).mapToDouble(f->f.doubleValue()).sum();

        return results;
    }

    public static double calc_sum_Subig(ConcurrentHashMap<String, Double> flow_map) {

        double results = flow_map.values().stream().filter(p->p>500000&&p<=1000000).mapToDouble(f->f.doubleValue()).sum();

        return results;
    }


    public static double calc_sum_Exbig(ConcurrentHashMap<String, Double> flow_map) {

        double results = flow_map.values().stream().filter(p->p> 1000000).mapToDouble(f->f.doubleValue()).sum();

        return results;
    }


}
