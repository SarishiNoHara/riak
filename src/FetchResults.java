import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class FetchResults {
    public static void main(String [] args) throws UnknownHostException, ExecutionException, InterruptedException {

        RiakClient client = RiakClient.newClient(10027, "127.0.0.1");
        Location location = new Location(new Namespace("Query1"),"A Woman of Distinction");

        FetchValue fv = new FetchValue.Builder(location).build();
        FetchValue.Response response = client.execute(fv);

        // Fetch object as String
        String value = response.getValue(String.class);
        System.out.println(value);

        client.shutdown();
    }
}
