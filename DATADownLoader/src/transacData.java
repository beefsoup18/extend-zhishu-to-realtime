import com.gta.qts.c2j.adaptee.structure.SSEL2_Transaction;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Transaction;
public class transacData {
    public Boolean SH_flag = false;
    public Boolean SZ_flag = false;
    public SSEL2_Transaction SH_data=null;
    public SZSEL2_Transaction SZ_data=null;

    public transacData(SSEL2_Transaction data){
        this.SH_data = data;
        this.SH_flag = true;
    }

    public transacData(SZSEL2_Transaction data){
        this.SZ_data = data;
        this.SZ_flag = true;
    }
}