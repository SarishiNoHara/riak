import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.*;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class SQL2Riak {

    //i:length of JSONArray
    //Return list of movie info (not full)
    public void query1(JSONArray array) throws JSONException, UnknownHostException, ExecutionException, InterruptedException {

        RiakClient client = RiakClient.newClient(10027, "127.0.0.1");

        for (int i = 0; i < 5; i++) {
            JSONObject rec = array.getJSONObject(i);

            Location location = new Location(new Namespace("Query1"), rec.getString("idmovies"));
            Location location1 = new Location(new Namespace("Query1"), rec.getString("title"));

            String loc = rec.toString(i);

            StoreValue sv_id = new StoreValue.Builder(loc).withLocation(location).build();
            StoreValue sv_title = new StoreValue.Builder(loc).withLocation(location1).build();

            client.execute(sv_id);
            client.execute(sv_title);
        }
        client.shutdown();

    }

    //Detailed actor information
    public void query2(JSONArray array) throws JSONException, UnknownHostException, ExecutionException, InterruptedException {
        RiakClient client = RiakClient.newClient(10027, "127.0.0.1");

        String OldStr = "";
        for (int i = 0; i < 13; i++) {

            JSONObject m1 = array.getJSONObject(i);

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
                Location location = new Location(new Namespace("Query2"), m1.getString("idactors"));
                Location location1 = new Location(new Namespace("Query2"), m1.getString("fname"));
                Location location2 = new Location(new Namespace("Query2"), m1.getString("lname"));

                StoreValue sv_id = new StoreValue.Builder(OldStr).withLocation(location).build();
                StoreValue sv_fname = new StoreValue.Builder(OldStr).withLocation(location1).build();
                StoreValue sv_lname = new StoreValue.Builder(OldStr).withLocation(location2).build();

                client.execute(sv_id);
                client.execute(sv_fname);
                client.execute(sv_lname);
            }
        }
        client.shutdown();
    }


    //Short actor statistics
    public void query3(JSONArray array) throws JSONException, UnknownHostException, ExecutionException, InterruptedException {

        RiakClient client = RiakClient.newClient(10027, "127.0.0.1");

        for (int i = 0; i < 5; i++) {
            JSONObject rec = array.getJSONObject(i);

            Location location = new Location(new Namespace("Query3"), rec.getString("idactors"));
            Location location1 = new Location(new Namespace("Query3"), rec.getString("fname"));
            Location location2 = new Location(new Namespace("Query3"), rec.getString("lname"));

            String loc = rec.toString(i);

            StoreValue sv_actId = new StoreValue.Builder(loc).withLocation(location).build();
            StoreValue sv_fname = new StoreValue.Builder(loc).withLocation(location1).build();
            StoreValue sv_lname = new StoreValue.Builder(loc).withLocation(location2).build();

            client.execute(sv_actId);
            client.execute(sv_fname);
            client.execute(sv_lname);
        }
        client.shutdown();
    }

    //Genre exploration ..too much output
    public void query4(JSONArray array) throws JSONException, UnknownHostException, ExecutionException, InterruptedException {
        RiakClient client = RiakClient.newClient(10027, "127.0.0.1");

        String OldStr = "";
        for (int i = 0; i < 5; i++) {

            JSONObject m1 = array.getJSONObject(i);
            System.out.println(m1);
            if (i == 0) {
                OldStr = m1.toString(i);
            } else {
                JSONObject m2 = array.getJSONObject(i - 1);
                if (m1.getString("genre").compareTo(m2.getString("genre")) == 0) {
                    String newStr = m1.toString(i);
                    OldStr = OldStr + newStr;
                } else {
                    OldStr = m1.toString(i);
                }

                Location location = new Location(new Namespace("Query4"), m1.getString("genre"));
                Location location1 = new Location(new Namespace("Query4"), m1.getString("year"));

                StoreValue genre = new StoreValue.Builder(OldStr).withLocation(location).build();
                StoreValue sv_year = new StoreValue.Builder(OldStr).withLocation(location1).build();

                client.execute(genre);
                client.execute(sv_year);
            }
        }
        client.shutdown();

    }

    //Genre statistics
    public void query5(JSONArray array) throws JSONException, UnknownHostException, ExecutionException, InterruptedException {
        RiakClient client = RiakClient.newClient(10027, "127.0.0.1");

        String OldStr = "";
        for (int i = 0; i < 100; i++) {

            JSONObject m1 = array.getJSONObject(i);

            if (i == 0) {
                OldStr = m1.toString(i);
            } else {
                JSONObject m2 = array.getJSONObject(i - 1);
                if (m1.getInt("year") == m2.getInt("year")) {
                    String newStr = m1.toString(i);
                    OldStr = OldStr + newStr;
                } else {
                    OldStr = m1.toString(i);
                }

                Location location = new Location(new Namespace("Query5"), m1.getString("year"));

                StoreValue sv_startYear = new StoreValue.Builder(OldStr).withLocation(location).build();

                client.execute(sv_startYear);
            }
        }
        client.shutdown();

    }

    public static void main(String args[]) throws Exception {

        String query1 = "SELECT m.idmovies, m.title, m.year, m.type, ak.location, count(s.idmovies) AS number FROM movies m\n" +
                "FULL JOIN aka_titles ak ON ak.idmovies=m.idmovies\n" +
                "FULL JOIN series s ON s.idmovies=m.idmovies\n" +
                "GROUP BY m.idmovies, m.title, m.year, m.type, ak.location\n" +
                "ORDER BY m.idmovies, m.year;";

        String query2 = "SELECT DISTINCT a.idactors,a.lname, a.fname, a.gender, m.title,m.year From actors a\n" +
                "JOIN acted_in ai  ON ai.idactors=a.idactors\n" +
                "JOIN movies m ON ai.idmovies=m.idmovies\n" +
                "WHERE a.idactors>=1 AND a.idactors<=5\n" +
                "ORDER BY a.idactors, m.year;";

        String query3 = "SELECT DISTINCT a.idactors,a.lname, a.fname, a.gender, count(m.idmovies) From actors a\n" +
                "JOIN acted_in ai  ON ai.idactors=a.idactors\n" +
                "JOIN movies m ON ai.idmovies=m.idmovies\n" +
                "WHERE a.idactors>=1 AND a.idactors<=5 \n" +
                "GROUP BY a.idactors,a.lname, a.fname, a.gender\n" +
                "ORDER BY a.idactors;";

        String query4 = "SELECT DISTINCT m.idmovies, m.title, g.genre, m.year, a.fname, a.lname, a.gender FROM  movies m\n" +
                "JOIN movies_genres mg ON mg.idmovies=m.idmovies\n" +
                "JOIN genres g ON g.idgenres=mg.idgenres\n" +
                "JOIN acted_in ai ON ai.idmovies=m.idmovies\n" +
                "JOIN actors a ON a.idactors=ai.idactors\n" +
                "WHERE m.year=2014\n" +
                "ORDER BY g.genre, m.year, m.title;";

        String query5 = "SELECT m.year,g.genre, count(m.idmovies) FROM genres g\n" +
                "JOIN movies_genres mg ON mg.idgenres=g.idgenres\n" +
                "JOIN movies m ON m.idmovies=mg.idmovies\n" +
                "WHERE m.year>=2009 AND m.year<=2013\n" +
                "GROUP BY g.idgenres, g.genre, m.year\n" +
                "ORDER BY m.year;";

        Connection c = null;
        Statement stmt = null;

        Class.forName("org.postgresql.Driver");
        c = DriverManager
                .getConnection("jdbc:postgresql://localhost:5432/imdb", "postgres", "123");
        c.setAutoCommit(false);
        System.out.println("Opened databse successfully");

        stmt = c.createStatement();

        ResultSet rs = stmt.executeQuery(query4);

        //Convert rs to JSON
        JSONArray array = Convertor.convertResultSetIntoJSON(rs);

        System.out.println(array);

        //Dump SQL query results into riak
        SQL2Riak newQuery = new SQL2Riak();
        newQuery.query4(array);

        rs.close();
        stmt.close();
        c.close();
    }

}