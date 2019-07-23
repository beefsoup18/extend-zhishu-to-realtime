package kafkaProc;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import wangzitian.realtime.ZhiShu;
public class ZhiShuHangQingSerializer implements Serializer<ZhiShu> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ZhiShu data) {
        try {
            DatumWriter<ZhiShu> signalRange0DatumWriter =
                    new SpecificDatumWriter<ZhiShu>(ZhiShu.class);
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