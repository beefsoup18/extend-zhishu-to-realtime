import java.io.UnsupportedEncodingException;
public class HangQingDistributor implements Runnable{
    private int threads_num;
    //    private ArrayList<ExecutorService> threads_array = new ArrayList<ExecutorService>();
    public HangQingDistributor() {
        this.threads_num = Codes.thread_num;
    }

    @Override
    public void run()
    {
        HangQingHolder data = null;
        int thread_id = 0;
        while(GLBuffer.working_flag) {
            data = GLBuffer.hangqingBuffer.poll();
            if(data==null){
                try {
                    Thread.sleep(1);
//                    System.out.println("HangQing Buffer Queue Empty! {}{}{}");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
//            System.out.println("HangQing data got");

            if(data.SZ_flag) {
                byte[] bytessymbol = data.SZ_data.Symbol;
                String code = byteArr2String(bytessymbol) + ".SZ";
                String tableName = "SZ"+byteArr2String(bytessymbol);
                thread_id = Codes.code_threadID.get(code);
                HangQing_SZwriter sztask = new HangQing_SZwriter(data.SZ_data, code, tableName,thread_id);
                Codes.threads_array.get(thread_id).execute(sztask);
            }
            else {
                byte[] bytessymbol = data.SH_data.Symbol;
                String code = byteArr2String(bytessymbol) + ".SH";
                String tableName = "SH"+byteArr2String(bytessymbol);
                thread_id = Codes.code_threadID.get(code);
                HangQing_SHwriter shtask = new HangQing_SHwriter(data.SH_data, code, tableName, thread_id);
                Codes.threads_array.get(thread_id).execute(shtask);
            }

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
