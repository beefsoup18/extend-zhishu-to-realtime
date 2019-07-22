import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;

class Codes {
    public static HashMap<String, Double> Weights = new HashMap<>();
    public static int code_cnt=0;
    public static int threads_num=1;
    public static ArrayList<String> topic4kafka = new ArrayList<>();
    public static HashMap<String, Integer> thread4codes = new HashMap<>();


    public static ArrayList<ExecutorService> threads_array= new ArrayList<>();
    public static String topicName = new String("RangeFlow300_201805");
    public static void setThreads_num(int num){
        Codes.threads_num = num;
        for(int i=0;i<num;i++){
            Codes.threads_array.add(Executors.newSingleThreadExecutor());
        }
    }

    public static void setSZweights(String filename) throws IOException {
        File file = new File(filename);
        if(!file.exists()) {
            System.out.println("SZ weight file not existed.");
            throw new FileNotFoundException();
        }

        FileReader reader = new FileReader(file);
        BufferedReader br = new BufferedReader(reader);

        String str = null;
        int thread_id = 0;
        while ((str=br.readLine())!=null) {
            String code = new String(str.split(",")[0]);
            Double weights = Double.valueOf(str.split(",")[1]) / 100;
            Codes.Weights.put(code +".SZ", weights);
            thread_id = Codes.code_cnt % Codes.threads_num;

            Codes.thread4codes.put(code + ".SZ", thread_id);
            Codes.topic4kafka.add("SZ"+code);

            Codes.code_cnt ++;

            GLBuffer.IN.put(code+".SZ", new ConcurrentHashMap<String, Double>());
            GLBuffer.OUT.put(code+".SZ", new ConcurrentHashMap<String, Double>());
        }
    }

    public static void setSHweights(String filename) throws IOException{
        File file = new File(filename);
        if(!file.exists()) {
            System.out.println("SH weight file not existed.");
            throw new FileNotFoundException();
        }

        FileReader reader = new FileReader(file);
        BufferedReader br = new BufferedReader(reader);

        String str = null;
        int thread_id = 0;
        while ((str=br.readLine())!=null) {
            String code = new String(str.split(",")[0]);
            Double weights = Double.valueOf(str.split(",")[1]) / 100;
            Codes.Weights.put(code +".SH", weights);

            thread_id = Codes.code_cnt % Codes.threads_num;

            Codes.thread4codes.put(code + ".SH", thread_id);
            Codes.topic4kafka.add("SH"+code);

            Codes.code_cnt ++;

            GLBuffer.IN.put(code+".SH", new ConcurrentHashMap<String, Double>());
            GLBuffer.OUT.put(code+".SH", new ConcurrentHashMap<String, Double>());

        }
    }

    public static void setTopicName(String filename) throws IOException{
        File file = new File(filename);
        if(!file.exists()){
            System.out.println("Topic name not specified by file");
            throw new FileNotFoundException();
        }

        if(file.length()==0){
            System.out.println("Topic file is Empty.");
            throw new FileNotFoundException();
        }

        FileReader reader = new FileReader(file);
        BufferedReader br = new BufferedReader(reader);
        String code_tp = br.readLine();
        Codes.topicName = new String(code_tp.trim());
        System.out.println("topic set, and topic is: " + Codes.topicName);

    }


}