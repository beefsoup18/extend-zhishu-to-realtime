import kafkaProc.ProducerCallback;
import org.apache.kafka.clients.producer.ProducerRecord;
import wangzitian.realtime.SignalRange0;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ScheduledTask implements Runnable{
    private SimpleDateFormat localformat;
    private ArrayList<Boolean> executedTickFlag = new ArrayList<>();
    private int tick_cnt = 0;
    private int tick_num = 0;

    public ScheduledTask() {
        this.localformat = new SimpleDateFormat("yyyyMMddHHmmss000");
        this.tick_num = TickUtils.ScheduledTick.size();
        for(int i=0;i<this.tick_num;i++){
            this.executedTickFlag.add(false);
        }
    }


    @Override
    public void run() {
        double low_in = 0;
        double middle_in = 0;
        double big_in = 0;
        double subig_in = 0;
        double exbig_in = 0;

        double low_out = 0;
        double middle_out = 0;
        double big_out = 0;
        double subig_out = 0;
        double exbig_out = 0;

        while (GLBuffer.working_flag) {

            try {
                Thread.sleep(100); //"prevent cpu overloading"
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Date now = new Date();
            String now_str = new String(this.localformat.format(now));
            Long local_now_num = Long.parseLong(now_str);
            Long remote_num = GLBuffer.remote_clock;

            Long tick_acc = TickUtils.ScheduledTick.get(this.tick_cnt);
            Long tick_lated = TickUtils.LatedTick.get(this.tick_cnt);
            System.out.println("tickcnt:" +this.tick_cnt + "  remote_clock:"+remote_num);
            if(local_now_num>tick_acc && !this.executedTickFlag.get(tick_cnt)) {
                System.out.println("I am in executedTick---------------------------------------------");
                if(local_now_num>tick_lated) {
                    GLBuffer.global_lock.lock();
                    ConcurrentHashMap<String, Boolean> executed_status = CodeResultsOfTick.executed_flag4code.get(this.tick_cnt);
                    for(Map.Entry<String, Boolean> entry: executed_status.entrySet()){
                        if(!entry.getValue()) {
                            calculatedCodeResult(entry.getKey());
                        }


                    }

                    calculateOverResult();
                    this.executedTickFlag.set(tick_cnt, true);
                    if(this.tick_cnt < this.tick_num-1){
                        this.tick_cnt += 1;
                    }

                    GLBuffer.global_lock.unlock();


                }
            }


        }
    }

    public void calculatedCodeResult(String code){
        ConcurrentHashMap<String, Double> codeFlow_in = GLBuffer.IN.get(code);
        ConcurrentHashMap<String, Double> codeFlow_out = GLBuffer.OUT.get(code);
        double weight = Codes.Weights.get(code);

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

        CodeResultsOfTick.lowInTickList.get(this.tick_cnt).put(code, low_in);
        CodeResultsOfTick.middleInTickList.get(this.tick_cnt).put(code, middle_in);
        CodeResultsOfTick.bigInTickList.get(this.tick_cnt).put(code,big_in);
        CodeResultsOfTick.suInTickList.get(this.tick_cnt).put(code, subig_in);
        CodeResultsOfTick.exInTickList.get(this.tick_cnt).put(code, exbig_in);

        CodeResultsOfTick.lowOutTickList.get(this.tick_cnt).put(code, low_out);
        CodeResultsOfTick.middleOutTickList.get(this.tick_cnt).put(code, middle_out);
        CodeResultsOfTick.bigOutTickList.get(this.tick_cnt).put(code,big_out);
        CodeResultsOfTick.suOutTickList.get(this.tick_cnt).put(code, subig_out);
        CodeResultsOfTick.exOutTickList.get(this.tick_cnt).put(code, exbig_out);

        CodeResultsOfTick.executed_flag4code.get(tick_cnt).put(code, true);

        if(this.tick_cnt < TickUtils.tick_num-1) {
            CodeResultsOfTick.tickCnt4code.put(code, this.tick_cnt + 1);
        }

    }

    public void calculateOverResult(){
        //double results = flow_map.values().stream().filter(p->p<=100000).mapToDouble(f->f.doubleValue()).sum();
        double low_in = CodeResultsOfTick.lowInTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();
        double middle_in = CodeResultsOfTick.middleInTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();
        double big_in = CodeResultsOfTick.bigInTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();
        double subig_in = CodeResultsOfTick.suInTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();
        double exbig_in = CodeResultsOfTick.exInTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();

        double low_out = CodeResultsOfTick.lowOutTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();
        double middle_out = CodeResultsOfTick.middleOutTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();
        double big_out = CodeResultsOfTick.bigOutTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();
        double subig_out = CodeResultsOfTick.suOutTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();
        double exbig_out = CodeResultsOfTick.exOutTickList.get(this.tick_cnt).values().stream().mapToDouble(f->f.doubleValue()).sum();

        try {
            FileWriter fw = new FileWriter("./"+TickUtils.ScheduledTick.get(this.tick_cnt)+".txt", true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("low_in; middle_in; big_in; subig_in; exbig_in; low_out; middle_out; big_out; subig_out; exbig_out; write_time");
            bw.newLine();
            bw.write(low_in + "; ");
            bw.write(middle_in+ "; ");
            bw.write(big_in+";");
            bw.write(subig_in +";");
            bw.write(exbig_in +";");

            bw.write(low_out +";");
            bw.write( middle_out +";");
            bw.write(big_out +";");
            bw.write(subig_out +";");
            bw.write(exbig_out +";");
            bw.write(String.valueOf(new Date()));
            bw.newLine();
            bw.flush();
            bw.close();
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        SignalRange0 sig = new SignalRange0();
        sig.setTick(TickUtils.ScheduledTick.get(this.tick_cnt));
        sig.setLowIn(low_in);
        sig.setMiddleIn(middle_in);
        sig.setBigIn(big_in);
        sig.setExbigIn(exbig_in);
        sig.setSubigIn(subig_in);

        sig.setLowOut(low_out);
        sig.setMiddleOut(middle_out);
        sig.setExbigOut(exbig_out);
        sig.setBigOut(big_out);
        sig.setSubigOut(subig_out);

        ProducerRecord<String, SignalRange0> record = new ProducerRecord<String, SignalRange0>(Codes.topicName, sig);
        KafkaSend.single_producer.send(record, new ProducerCallback());

        System.out.println("signal send");

    }

}
