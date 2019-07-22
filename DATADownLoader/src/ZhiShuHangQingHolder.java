import com.gta.qts.c2j.adaptee.structure.SSEL2_Index;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Index;

public class ZhiShuHangQingHolder {
    public Boolean SH_flag = false;
    public Boolean SZ_flag = false;
    public SSEL2_Index SH_data=null;
    public SZSEL2_Index SZ_data=null;

    public ZhiShuHangQingHolder(SSEL2_Index data){
        this.SH_data = data;
        this.SH_flag = true;
    }

    public ZhiShuHangQingHolder(SZSEL2_Index data){
        this.SZ_data = data;
        this.SZ_flag = true;
    }
}