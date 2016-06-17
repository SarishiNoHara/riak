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
        String query2 = "SELECT DISTINCT a.idactors,a.lname, a.fname, a.gender, m.title,m.year From actors a JOIN acted_in ai  ON ai.idactors=a.idactors JOIN movies m ON ai.idmovies=m.idmovies WHERE a.idactors>=1 AND a.idactors<=5 ORDER BY a.idactors, m.year;";

        String query3 = "";

        ResultSet rs = stmt.executeQuery(query2);

        Convertor a = new Convertor();
        JSONArray array = a.convertResultSetIntoJSON(rs);

        System.out.println(array);
        System.out.println("kkk");

        RiakClient client = RiakClient.newClient(10027, "127.0.0.1");

        String OldStr = "";
        for (int i = 0; i < 13; i++) {

            JSONObject m1 = array.getJSONObject(i);
            System.out.println(m1);

            if (i == 0) {
                OldStr = m1.toString(i);
            } else {
                JSONObject m2 = array.getJSONObject(i - 1);
                if (m1.getInt("idactors") == m2.getInt("idactors")) {
                    String newStr = m1.toString(i);
                    OldStr = OldStr + "\n" + newStr;
                } else {
                    OldStr = m1.toString(i);
                }
                Location location = new Location(new Namespace("Query1"), m1.getString("idactors"));
                Location location1 = new Location(new Namespace("Query1"), m1.getString("fname"));

                StoreValue sv_id = new StoreValue.Builder(OldStr).withLocation(location).build();
                StoreValue sv_title = new StoreValue.Builder(OldStr).withLocation(location1).build();

                StoreValue.Response svResponse = client.execute(sv_id);
                StoreValue.Response svResponse1 = client.execute(sv_title);
            }
        }
            client.shutdown();
            rs.close();
            stmt.close();
            c.close();

        }
    }



