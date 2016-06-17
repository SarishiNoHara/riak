import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;

public class PostgreSQLJDBC {
    public static void main(String args[]) throws Exception {
        Connection c = null;
        Statement stmt = null;

        Class.forName("org.postgresql.Driver");
        c = DriverManager
                .getConnection("jdbc:postgresql://localhost:5432/imdb", "postgres", "123");
        c.setAutoCommit(false);
        System.out.println("Opened databse successfully");

        stmt = c.createStatement();
        String query1 = "SELECT m.idmovies, m.title, m.year, m.type, ak.location, count(s.idmovies) AS number FROM movies m FULL JOIN aka_titles ak ON ak.idmovies=m.idmovies\n" +
                "FULL JOIN series s ON s.idmovies=m.idmovies GROUP BY m.idmovies, m.title, m.year, m.type, ak.location ORDER BY m.idmovies, m.year;";
        String query2 = "SELECT * FROM actors WHERE idactors>=1 AND idactors<=5 ORDER BY idactors ";
        



        ResultSet rs = stmt.executeQuery(query2);

        Convertor a = new Convertor();
        JSONArray movies = a.convertResultSetIntoJSON(rs);

          System.out.println(movies);
         System.out.println("kkk");

        RiakClient client = RiakClient.newClient(10027, "127.0.0.1");

        for (int i = 0; i < 5; i++) {
            JSONObject rec = movies.getJSONObject(i);

            Location location = new Location(new Namespace("Query1"), rec.getString("idactors"));
            Location location1 = new Location(new Namespace("Query1"), rec.getString("fname"));

            String loc = rec.toString(i);

            StoreValue sv_id = new StoreValue.Builder(loc).withLocation(location).build();
            StoreValue sv_title = new StoreValue.Builder(loc).withLocation(location1).build();

            StoreValue.Response svResponse = client.execute(sv_id);
            StoreValue.Response svResponse1 = client.execute(sv_title);
        }
            client.shutdown();
            rs.close();
            stmt.close();
            c.close();

        }
    }

