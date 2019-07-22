package kafkaProc;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;
import wangzitian.realtime.HangQing;

public class HangQingDesrializer implements Deserializer<HangQing>{

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public HangQing deserialize(String topic, byte[] data) {
        try{
            DatumReader<HangQing> signalReader = new SpecificDatumReader<HangQing>(HangQing.class);
            BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);

            HangQing sigGot = signalReader.read(new HangQing(), binaryDecoder);
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