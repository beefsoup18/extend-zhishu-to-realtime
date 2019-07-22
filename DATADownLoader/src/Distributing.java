import java.util.concurrent.ExecutorService;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.io.UnsupportedEncodingException;
public class Distributing implements Runnable{
    private int threads_num;
    private ArrayList<ExecutorService> threads_array = new ArrayList<ExecutorService>();
    public Distributing() {
        this.threads_num = Codes.thread_num;
        for(int i = 0;i<this.threads_num;i++){
            this.threads_array.add(Executors.newSingleThreadExecutor());

        }
    }

    @Override
    public void run()
    {
        transacData data = null;
        int thread_id = 0;
        while(GLBuffer.working_flag) {
            data = GLBuffer.databuffer.poll();
            if(data==null){
                try {
                    Thread.sleep(1);
                    System.out.println("Buffer Queue Empty!");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
//            System.out.println("data got");
            if(data.SZ_flag) {
                byte[] bytessymbol = data.SZ_data.Symbol;
                String code = byteArr2String(bytessymbol) + ".SZ";
                String tableName = "SZ"+byteArr2String(bytessymbol);
                thread_id = Codes.code_threadID.get(code);
                SZwrite sztask = new SZwrite(data.SZ_data, code, tableName,thread_id);
                this.threads_array.get(thread_id).execute(sztask);
            }
            else {
                byte[] bytessymbol = data.SH_data.Symbol;
                String code = byteArr2String(bytessymbol) + ".SH";
                String tableName = "SH"+byteArr2String(bytessymbol);
                thread_id = Codes.code_threadID.get(code);
                SHwrite shtask = new SHwrite(data.SH_data, code, tableName, thread_id);
                this.threads_array.get(thread_id).execute(shtask);
            }

        }

        try {
            Thread.sleep(1000*10);
            for(int i=0;i<this.threads_num;i++){
                this.threads_array.get(i).shutdown();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String byteArr2String(byte[] bytes){
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
