import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class TickUtils {
    public static ArrayList<Long> ScheduledTick = new ArrayList<>();
    public static ArrayList<Long> LatedTick = new ArrayList<>();

    public static int tick_pointer = 0;
    public static int tick_num = 0;

    public static String string_today;

    public static void getScheduledTickFromTXT(String filename) throws IOException
    {
        File file =new File(filename);
        if(!file.exists()){
            System.out.println("getScheduledTick error. file not existed");
            throw new FileNotFoundException();
        }

        FileReader reader = new FileReader(file);
        BufferedReader br = new BufferedReader(reader);
        Date today = new Date();
        SimpleDateFormat todayFormat = new SimpleDateFormat("yyyyMMdd");
        String today_str = todayFormat.format(today);
        System.out.println("Today is " + today_str);
        String str = null;
        while((str=br.readLine())!=null) {
            String whole_time_ = new String(today_str + str);

            System.out.println("Tick:" + whole_time_);

            Long whole_time = Long.parseLong(whole_time_);
            TickUtils.ScheduledTick.add(whole_time);
            TickUtils.tick_num ++;
        }
        br.close();reader.close();
    }

    public static void getDelayTickFromTXT(String filename) throws IOException, ParseException
    {
        File file =new File(filename);
        if(!file.exists()){
            System.out.println("getScheduledTick error. file not existed");
            throw new FileNotFoundException();
        }

        FileReader reader = new FileReader(file);
        BufferedReader br = new BufferedReader(reader);
        Date today = new Date();
        SimpleDateFormat todayFormat = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat whole_format = new SimpleDateFormat("yyyyMMddHHmmss");
        String today_str = todayFormat.format(today);
        System.out.println("Today is " + today_str);
        String str = null;
        Date time_ = new Date();
        while((str=br.readLine())!=null) {
            String whole_time_ = new String(today_str + str);

            time_ = whole_format.parse(whole_time_);
            Date after_time_ = new Date(time_.getTime() + 12000);
            String whole_dely = whole_format.format(after_time_)+"000";
            Long whole_time = Long.parseLong(whole_dely);
            TickUtils.LatedTick.add(whole_time);
        }
        br.close();reader.close();

    }

    public static void setString_today(){
        SimpleDateFormat format_today = new SimpleDateFormat("yyyyMMdd");
        TickUtils.string_today = format_today.format(new Date());
    }
}
