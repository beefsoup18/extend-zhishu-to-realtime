package kafkaProc;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import wangzitian.realtime.Transactions;

import java.io.ByteArrayInputStream;
import java.util.Map;
import wangzitian.realtime.Transactions;

public class TransacDeserilizer implements Deserializer<Transactions>{

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Transactions deserialize(String topic, byte[] data) {
        try{
            DatumReader<Transactions> signalReader = new SpecificDatumReader<Transactions>(Transactions.class);
            BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);

            Transactions sigGot = signalReader.read(new Transactions(), binaryDecoder);
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