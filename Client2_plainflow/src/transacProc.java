import wangzitian.realtime.Transactions;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
public class transacProc implements Runnable{
    private int thread_id;
    private Transactions data;
    private long time_stamp;
    private double weight;
    public transacProc(Transactions data, double weight, int thread_id, long time_stamp){
        this.thread_id = thread_id;
        this.data = data;
        this.time_stamp = time_stamp;
        this.weight = weight;
    }


    @Override
    public void run() {


        String code = data.getSymbol().toString();
        String buyID = String.valueOf(data.getBuyID());
        String sellID = String.valueOf(data.getSellID());
        double weight = Codes.Weights.get(code);
        double total_money = data.getTradeAmount();

//        System.out.println("In thread, " + "Code:" + code + "  ,tradeTime:" + data.getTradeTime() + "  ,buyID：" + buyID + "  ,sellID:" + sellID + "  ,\nTradeAmount:" +total_money);


        ConcurrentHashMap<String,Double> codeFlow_in = GLBuffer.IN.get(code);
        if(codeFlow_in.get(buyID) == null){
            codeFlow_in.put(buyID, total_money);
        }else {
            codeFlow_in.put(buyID, total_money + codeFlow_in.get(buyID));
        }
        GLBuffer.IN.put(code, codeFlow_in);


        ConcurrentHashMap<String, Double> codeFlow_out = GLBuffer.OUT.get(code);
        if(codeFlow_out.get(sellID)==null){
            codeFlow_out.put(sellID, total_money);
        }else {
            codeFlow_out.put(sellID, total_money + codeFlow_out.get(sellID));
        }
        GLBuffer.OUT.put(code, codeFlow_out);

        Long code_tradeTime;
        if(code.split("\\.")[1].equals("SH")) {
//            System.out.println("in SH");
            String code_tradeTime_str = String.valueOf(data.getTradeTime());
            if(code_tradeTime_str.length()==8)
                code_tradeTime_str = TickUtils.string_today + "0" + code_tradeTime_str;
            else
                code_tradeTime_str = TickUtils.string_today  + code_tradeTime_str;
            code_tradeTime = Long.valueOf(code_tradeTime_str);
        }else{
            code_tradeTime = data.getTradeTime();
        }
//        System.out.println("code:" + code + ",  formatted trade time:" + code_tradeTime);
        GLBuffer.remote_clock = Math.max(GLBuffer.remote_clock, code_tradeTime);

        int code_tick_pointer = CodeResultsOfTick.tickCnt4code.get(code);
        if(code_tradeTime > TickUtils.ScheduledTick.get(code_tick_pointer)
                && (!CodeResultsOfTick.executed_flag4code.get(code_tick_pointer).get(code))) {

            double low_in = CalcRangeSum.calc_sum_low(codeFlow_in) * weight;
            double middle_in = CalcRangeSum.calc_sum_middle(codeFlow_in)* weight;
            double big_in = CalcRangeSum.calc_sum_big(codeFlow_in)* weight;
            double subig_in = CalcRangeSum.calc_sum_Subig(codeFlow_in)* weight;
            double exbig_in = CalcRangeSum.calc_sum_Exbig(codeFlow_in)* weight;

            double low_out = CalcRangeSum.calc_sum_low(codeFlow_out)* weight;
            double middle_out = CalcRangeSum.calc_sum_middle(codeFlow_out)* weight;
            double big_out = CalcRangeSum.calc_sum_big(codeFlow_out)* weight;
            double subig_out = CalcRangeSum.calc_sum_Subig(codeFlow_out)* weight;
            double exbig_out = CalcRangeSum.calc_sum_Exbig(codeFlow_out)* weight;

            CodeResultsOfTick.lowInTickList.get(code_tick_pointer).put(code, low_in);
            CodeResultsOfTick.middleInTickList.get(code_tick_pointer).put(code, middle_in);
            CodeResultsOfTick.bigInTickList.get(code_tick_pointer).put(code,big_in);
            CodeResultsOfTick.suInTickList.get(code_tick_pointer).put(code, subig_in);
            CodeResultsOfTick.exInTickList.get(code_tick_pointer).put(code, exbig_in);

            CodeResultsOfTick.lowOutTickList.get(code_tick_pointer).put(code, low_out);
            CodeResultsOfTick.middleOutTickList.get(code_tick_pointer).put(code, middle_out);
            CodeResultsOfTick.bigOutTickList.get(code_tick_pointer).put(code,big_out);
            CodeResultsOfTick.suOutTickList.get(code_tick_pointer).put(code, subig_out);
            CodeResultsOfTick.exOutTickList.get(code_tick_pointer).put(code, exbig_out);

            CodeResultsOfTick.executed_flag4code.get(code_tick_pointer).put(code, true);
            System.out.println("Tick:"+TickUtils.ScheduledTick.get(code_tick_pointer) +
                    " Code:" + code + "calculated, now ");
            if(code_tick_pointer < TickUtils.tick_num-1) {
                CodeResultsOfTick.tickCnt4code.put(code, code_tick_pointer + 1);
            }



        }

        writeTransac.writeTransac2txt(data, time_stamp);




    }
}
