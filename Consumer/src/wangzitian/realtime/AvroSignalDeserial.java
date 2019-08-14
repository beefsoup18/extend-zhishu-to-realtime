package wangzitian.realtime;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;

public class AvroSignalDeserial implements Deserializer<SignalRange0>{
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public SignalRange0 deserialize(String topic, byte[] data) {
        try{
            DatumReader<SignalRange0> signalReader = new SpecificDatumReader<SignalRange0>(SignalRange0.class);
            BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);

            SignalRange0 sigGot = signalReader.read(new SignalRange0(), binaryDecoder);
            return  sigGot;
        }catch (Exception e){
            e.printStackTrace();
            throw new SerializationException("error when deserializing Customer to byte[]: " + e);
        }
    }

    @Override
    public void close(){

    }

}
