package PostgreSQL_Repair;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static java.util.Arrays.asList;

/**
 * Created by i.Heldyieu on 31.10.2016.
 * Sometimes you can catch specific problem "invalid memory alloc request size" working with PostgreSQL. Therefore you cannot make backup
 * This utilite search these bad rows and try to delete them.
 */
public class Fixer
{
    protected static String urlDatabase = "";
    protected static String port = "";
    protected static String server = "";
    protected static String dataBase = "";
    protected static String user = "";
    protected static String password = "";
    protected static List<String> variants = asList("1","2","3","4","5");
    protected static HashMap<Integer, String> databaseMap = new HashMap<>();
    protected static HashMap<Integer, String> rows = new HashMap<>();

    public static void main(String[] args) throws SQLException, InterruptedException
    {
        databaseMap.put(0,"fst_rcimages:i_id");
        databaseMap.put(1,"fstpr_sh_intr:sh_id");
        databaseMap.put(2,"fstpr_us_intr:us_id");
        databaseMap.put(3,"fstpr_http_post:s_id");
        Connection check = null;
        try
        {
            Class.forName("org.postgresql.Driver");

            BufferedReader q = new BufferedReader(new InputStreamReader(System.in));

            //try to connect database with input parameters from user console
            check = getConnection(q);

            //check if specific table exist? check number of rows in this table : print "table doesn't exist" to a user
            getRowsFromCurrentTable(check);

            System.out.println();
            boolean state=false;
            String temp=null;
            while (!state)
            {
                search.order.clear();
                System.out.println("Choose table to repair:\n1 - fst_rcimages: "+rows.get(0)+" rows\n2 - fstpr_sh_intr: "+rows.get(1)+" rows\n3 - fstpr_us_intr: "+rows.get(2)+" rows\n4 - fstpr_http_post: "+rows.get(3)+" rows\n5 - all this tables");
                String choice ="";
                try
                {
                    choice = q.readLine().trim();
                }
                catch (IOException e)
                {
                    System.out.println(e);
                }
                while (!variants.contains(choice))
                {
                    System.out.println("Wrong input table. Please select correct table");
                    try
                    {
                        choice = q.readLine().trim();
                    }
                    catch (NumberFormatException e)
                    {
                        System.out.println("Input correct value");
                    }
                    catch (IOException e)
                    {
                        System.out.println(e);
                    }
                }
                switch (choice)
                {
                    case "1":
                        new search("fst_rcimages", "i_id", DriverManager.getConnection(urlDatabase, user, password));
                        break;
                    case "2":
                        new search("fstpr_sh_intr","sh_id", DriverManager.getConnection(urlDatabase, user, password));
                        break;
                    case "3":
                       new search("fstpr_us_intr","us_id", DriverManager.getConnection(urlDatabase, user, password));
                        break;
                    case "4":
                        new search("fstpr_http_post", "s_id", DriverManager.getConnection(urlDatabase, user, password));
                        break;
                    case "5":
                            new search("fst_rcimages", "i_id", DriverManager.getConnection(urlDatabase, user, password));
                            new search("fstpr_sh_intr","sh_id", DriverManager.getConnection(urlDatabase, user, password));
                            new search("fstpr_us_intr","us_id", DriverManager.getConnection(urlDatabase, user, password));
                            new search("fstpr_http_post", "s_id", DriverManager.getConnection(urlDatabase, user, password));
                        break;
                }
                ExecutorService executor = Executors.newFixedThreadPool(search.order.size());
                for (int i = 0; i <search.order.size() ; i++)
                {
                    Runnable worker = search.order.get(i);
                    executor.execute(worker);
                }
                executor.shutdown();
                while (!executor.isTerminated()){
                }

                System.out.println("Check another table(s)? y/n");
                temp=q.readLine().toLowerCase();
                while (!temp.equals("y") && !temp.equals("n") && !temp.equals(""))
                {
                    System.out.println("Input correct letter!");
                    temp=q.readLine().toLowerCase();
                    if (temp.equals("n")){
                        state=true;break;}
                }
                if (temp.equals("n")){
                    state=true;}
            }
        }
        catch (ClassNotFoundException e){
            System.out.println("Couldn't find correct Database Driver");
            System.out.println(e);
        }
        catch (Exception e)
        {
            System.out.println(e);
        }
    }

    protected static void getRowsFromCurrentTable(Connection check) throws SQLException
    {
        for (int i = 0; i <databaseMap.size() ; i++)
            {
                Statement st = check.createStatement();
                String [] temp = databaseMap.get(i).split(":");
                String localBadTable = temp[0];
                String localIdColumn = temp[1];
                try
                {
                    ResultSet rs = st.executeQuery("SELECT " + localIdColumn + " FROM " + localBadTable + " ORDER BY " + localIdColumn + " DESC limit 1");
                    while (rs.next())
                    {
                        rows.put(i,String.valueOf(rs.getInt(localIdColumn)));
                    }
                }
                catch (SQLException e)
                {
                    String temp1=e.toString();
                    if (temp1.contains("не существует") || temp1.contains("does not exist"))
                    {
                        rows.put(i,"table does not exist");
                    } else
                    {
                        System.out.println(e);
                    }
                }
            }
    }

    protected static Connection getConnection(BufferedReader q) throws IOException
    {
        Connection check;
        while (true)
        {
            System.out.print("Server to connect:");
            server = q.readLine().trim();
            System.out.print("Port:");
            port = q.readLine().trim();
            System.out.print("Database to connect:");
            dataBase = q.readLine().trim();
            System.out.print("Username:");
            user = q.readLine().trim();
            System.out.print("Password:");
            char[] passString = System.console().readPassword();
            password = new String(passString).trim();
            urlDatabase = "jdbc:postgresql://" + server + ":" + port + "/" + dataBase;
            try
            {
                check = DriverManager.getConnection(urlDatabase, user, password);
                System.out.println("Database is connected successfully!");
                break;
            }
            catch (SQLException e)
            {
                System.out.println("Cannot connect to the database! Check your input values!");
                System.out.println();
            }
        }
        return check;
    }
}