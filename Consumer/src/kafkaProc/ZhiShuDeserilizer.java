package kafkaProc;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;

import wangzitian.realtime.ZhiShu;

public class ZhiShuDeserilizer implements Deserializer<ZhiShu>{

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public ZhiShu deserialize(String topic, byte[] data) {
        try{
            DatumReader<ZhiShu> signalReader = new SpecificDatumReader<ZhiShu>(ZhiShu.class);
            BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);

            ZhiShu sigGot = signalReader.read(new ZhiShu(), binaryDecoder);
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