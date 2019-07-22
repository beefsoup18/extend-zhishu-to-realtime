import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Codes {
    public static int code_cnt = 0;
    public static int thread_num = 1;
    public static HashMap<String, Integer> code_threadID = new HashMap<String,Integer>();
    public static String SZ_string = new String();
    public static String SH_string = new String();
    public static ArrayList<String> code_talbe_names = new ArrayList<String>();

    public static ArrayList<ExecutorService> threads_array = new ArrayList<ExecutorService>();

    public static void setThreadsNum(int thread_num){
        Codes.thread_num = thread_num;
    }

    public static void setSZlist(String filename) throws IOException {
        File file = new File(filename);
        if(!file.exists()){
            System.out.println("SZ file not existed");
            throw new FileNotFoundException();
        }


        FileReader reader = new FileReader(file);
        BufferedReader br = new BufferedReader(reader);

        String str = null;
        StringBuilder sb = new StringBuilder();
        while((str=br.readLine())!=null) {
            //System.out.println(str);
            String code = new String(str.split(",")[0]);
            Codes.code_talbe_names.add("SZ" + code);
            sb.append(code + ",");
            int thread_id = Codes.code_cnt % Codes.thread_num;
            Codes.code_threadID.put(code+".SZ", thread_id);
            Codes.code_cnt += 1;

        }

        Codes.SZ_string = sb.toString();
        br.close();reader.close();
    }


    public static void setSHlist(String filename) throws IOException {
        File file = new File(filename);
        if(!file.exists()){
            System.out.println("SH file not existed");
            throw new FileNotFoundException();
        }


        FileReader reader = new FileReader(file);
        BufferedReader br = new BufferedReader(reader);

        String str = null;
        StringBuilder sb = new StringBuilder();
        while((str=br.readLine())!=null) {
            //System.out.println(str);

            String code = new String(str.split(",")[0]);
            Codes.code_talbe_names.add("SH" + code);
            sb.append(code + ",");

            int thread_id = Codes.code_cnt % Codes.thread_num;
            Codes.code_threadID.put(code+".SH", thread_id);
            Codes.code_cnt += 1;

        }

        Codes.SH_string = sb.toString();
        br.close();reader.close();
    }

    public static void buildGLThreadsPool() {
        int threads_num = Codes.thread_num;
        for(int i = 0;i< threads_num;i++){
            Codes.threads_array.add(Executors.newSingleThreadExecutor());

        }

    }

    public static void closeGLThreadPool() {
        try {
            Thread.sleep(1000*10);
            for(int i=0;i<Codes.thread_num;i++){
                Codes.threads_array.get(i).shutdown();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}