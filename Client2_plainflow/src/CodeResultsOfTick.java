import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
public class CodeResultsOfTick {
    public static ArrayList<ConcurrentHashMap<String, Double>> lowInTickList = new ArrayList<>();
    public static ArrayList<ConcurrentHashMap<String, Double>> middleInTickList = new ArrayList<>();
    public static ArrayList<ConcurrentHashMap<String, Double>> bigInTickList = new ArrayList<>();
    public static ArrayList<ConcurrentHashMap<String, Double>> suInTickList = new ArrayList<>();
    public static ArrayList<ConcurrentHashMap<String, Double>> exInTickList = new ArrayList<>();

    public static ArrayList<ConcurrentHashMap<String, Double>> lowOutTickList = new ArrayList<>();
    public static ArrayList<ConcurrentHashMap<String, Double>> middleOutTickList = new ArrayList<>();
    public static ArrayList<ConcurrentHashMap<String, Double>> bigOutTickList = new ArrayList<>();
    public static ArrayList<ConcurrentHashMap<String, Double>> suOutTickList = new ArrayList<>();
    public static ArrayList<ConcurrentHashMap<String, Double>> exOutTickList = new ArrayList<>();

    public static ConcurrentHashMap<String, Integer> tickCnt4code = new ConcurrentHashMap<>();
    public static ArrayList<ConcurrentHashMap<String, Boolean>> executed_flag4code = new ArrayList<>();

    public static void initCodeResultsOfTick(ArrayList<Long>TickList, HashMap<String, Double> WeightList) {
        for(int i=0;i<TickList.size();i++) {
            ConcurrentHashMap<String, Double> Code_LowIN_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_LowIN_pair, WeightList);
            CodeResultsOfTick.lowInTickList.add(Code_LowIN_pair);

            ConcurrentHashMap<String, Double> Code_middleIN_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_middleIN_pair, WeightList);
            CodeResultsOfTick.middleInTickList.add(Code_middleIN_pair);

            ConcurrentHashMap<String, Double> Code_bigIN_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_bigIN_pair, WeightList);
            CodeResultsOfTick.bigInTickList.add(Code_bigIN_pair);

            ConcurrentHashMap<String, Double> Code_suIN_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_suIN_pair, WeightList);
            CodeResultsOfTick.suInTickList.add(Code_suIN_pair);

            ConcurrentHashMap<String, Double> Code_exIN_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_exIN_pair, WeightList);
            CodeResultsOfTick.exInTickList.add(Code_exIN_pair);

            //low-out

            ConcurrentHashMap<String, Double> Code_LowOUT_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_LowOUT_pair, WeightList);
            CodeResultsOfTick.lowOutTickList.add(Code_LowOUT_pair);

            ConcurrentHashMap<String, Double> Code_middleOUT_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_middleOUT_pair, WeightList);
            CodeResultsOfTick.middleOutTickList.add(Code_middleOUT_pair);

            ConcurrentHashMap<String, Double> Code_bigOUT_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_bigOUT_pair, WeightList);
            CodeResultsOfTick.bigOutTickList.add(Code_bigOUT_pair);

            ConcurrentHashMap<String, Double> Code_suOUT_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_suOUT_pair, WeightList);
            CodeResultsOfTick.suOutTickList.add(Code_suOUT_pair);

            ConcurrentHashMap<String, Double> Code_exOUT_pair = new ConcurrentHashMap<>();
            initalTickResultMap(Code_exOUT_pair, WeightList);
            CodeResultsOfTick.exOutTickList.add(Code_exOUT_pair);

            ConcurrentHashMap<String, Boolean> Code_executed_pair = new ConcurrentHashMap<>();
            initalTickExecutedMap(Code_executed_pair, WeightList);
            CodeResultsOfTick.executed_flag4code.add(Code_executed_pair);

        }

        for(Map.Entry<String, Double> entry:WeightList.entrySet()){
            CodeResultsOfTick.tickCnt4code.put(entry.getKey(), 0);
        }

    }


    public static void initalTickResultMap(ConcurrentHashMap<String, Double> Map, HashMap<String, Double>weight_list){
        for(Map.Entry<String, Double> entry:weight_list.entrySet()) {
            Map.put(entry.getKey(), 0.0);
        }

    }

    public static void initalTickExecutedMap(ConcurrentHashMap<String, Boolean> Map, HashMap<String, Double>weight_list){
        for(Map.Entry<String, Double> entry:weight_list.entrySet()) {
            Map.put(entry.getKey(), false);
        }

    }


}
