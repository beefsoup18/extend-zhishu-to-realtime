

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils
{
    public static Date getEndofDay(){
        Date now = new Date();
        String now_str = GLBuffer.df_day.format(now);
        String end_str = now_str + ",23:00:00";
        Date end = null;
        try {
            end = GLBuffer.df_time.parse(end_str);
            return end;
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(-3);
        }
        return end;
    }
}
