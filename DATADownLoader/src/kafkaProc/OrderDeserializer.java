package kafkaProc;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import wangzitian.realtime.SZorder;
import wangzitian.realtime.Transactions;

import java.io.ByteArrayInputStream;
import java.util.Map;
import wangzitian.realtime.Transactions;

public class OrderDeserializer implements Deserializer<SZorder>{

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public SZorder deserialize(String topic, byte[] data) {
        try{
            DatumReader<SZorder> signalReader = new SpecificDatumReader<SZorder>(SZorder.class);
            BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);

            SZorder sigGot = signalReader.read(new SZorder(), binaryDecoder);
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