package kafkaProc;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import wangzitian.realtime.Transactions;

import java.io.ByteArrayOutputStream;
import java.util.Map;
public class TransacSerilizer implements Serializer<Transactions> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Transactions data) {
        try {
            DatumWriter<Transactions> signalRange0DatumWriter =
                    new SpecificDatumWriter<Transactions>(Transactions.class);
            ByteArrayOutputStream signalOutputStream =
                    new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(signalOutputStream, null);

            signalRange0DatumWriter.write(data, binaryEncoder);
            byte[] serialized = signalOutputStream.toByteArray();
            return serialized;
        }catch(Exception e){
            e.printStackTrace();
            throw new SerializationException("Error when serilizing data:" + e);
        }
    }

    @Override
    public void close(){

    }
}