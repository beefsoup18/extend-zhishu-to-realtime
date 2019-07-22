import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class MongoDB {
    public static String mongoIP = new String("localhost");
    public static String dbname = new String("test");

    public static ArrayList<MongoClient> mongoPool = new ArrayList<MongoClient>();
    public static ArrayList<MongoDatabase> dbPool = new ArrayList<MongoDatabase>();

    public static void createTables(){
        MongoClient mongo_cli = new MongoClient(MongoDB.mongoIP, 27017);
        MongoDatabase database = mongo_cli.getDatabase(MongoDB.dbname);
        for (int i=0;i<Codes.code_talbe_names.size();i++) {
            System.out.println("Names:" + Codes.code_talbe_names.get(i));

            try {
                database.createCollection(Codes.code_talbe_names.get(i));
            }
            catch (com.mongodb.MongoCommandException e) {
                e.printStackTrace();
            }

            MongoCollection<Document> collection = database.getCollection(Codes.code_talbe_names.get(i));
            collection.createIndex(Indexes.ascending("sellorderid", "buyorderid", "trademoney"));
//            collection.createIndex(Indexes.ascending("sellorderid"));
//            collection.createIndex(Indexes.ascending("buyorderid"));
//            collection.createIndex(Indexes.ascending("trademoney"));

//            collection.createIndex(Indexes.ascending("expiredAt"), new IndexOptions().expireAfter(0L, TimeUnit.SECONDS));

            System.out.println(Codes.code_talbe_names.get(i)+" created.");


        }
        mongo_cli.close();
    }

    public static void buildMongoClients() {
        int thread_num = Codes.thread_num;
        for(int i=0;i<thread_num;i++){

            try {
                MongoDB.mongoPool.
                        add(new MongoClient(MongoDB.mongoIP, 27017));

                MongoDB.dbPool.
                        add(MongoDB.mongoPool.get(i).getDatabase(MongoDB.dbname));
                System.out.println(i + "th mongo client created.");
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(-1);
            }
        }

    }

    public static void closeMongoClients(){
        for(int i=0;i<MongoDB.mongoPool.size();i++){
            MongoDB.mongoPool.get(i).close();
        }
    }


}
