import com.gta.qts.c2j.adaptee.structure.SSEL2_Quotation;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Quotation;

public class HangQingHolder {
    public Boolean SH_flag = false;
    public Boolean SZ_flag = false;
    public SSEL2_Quotation SH_data=null;
    public SZSEL2_Quotation SZ_data=null;

    public HangQingHolder(SSEL2_Quotation data){
        this.SH_data = data;
        this.SH_flag = true;
    }

    public HangQingHolder(SZSEL2_Quotation data){
        this.SZ_data = data;
        this.SZ_flag = true;
    }
}