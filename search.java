package PostgreSQL_Repair.PostgreSQL_Repair;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by i.Heldyieu on 02.11.2016.
 */
public class search implements Runnable
{
    protected  int lastRecord;
    protected  int badRows;
    protected  int deletedRows;
    protected  String localBadTable;
    protected  String localIdColumn;
    protected  Connection db;
    protected  boolean tableExists =true;
    protected static ArrayList<search> order = new ArrayList<>();

    public search(String badTable, String idColumn, Connection db)
    {
        localBadTable=badTable;
        localIdColumn=idColumn;
        this.db=db;
        order.add(this);
    }

    @Override
    public void run()
    {
            long start = new Date().getTime();
            Statement st = null;
            try
            {
                st = db.createStatement();

            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
            ResultSet rs = null;
            try
            {
                rs = st.executeQuery("SELECT " + localIdColumn + " FROM " + localBadTable + " ORDER BY " + localIdColumn + " DESC limit 1");
            }
            catch (SQLException e)
            {
                String temp=e.toString();
                if (temp.contains("не существует") || temp.contains("does not exist"))
                {
                    System.out.println(localBadTable + " does not exist");
                    tableExists = false;

                } else
                {
                    System.out.println(e);
                }
            }
        if (tableExists)
        {
            try
            {
                while (rs.next())
                {
                    lastRecord = rs.getInt(localIdColumn);
                    System.out.println("Last record in the table " + localBadTable + " is: " + lastRecord);
                }
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
            for (int i = 1; i <= lastRecord; i++)
            {
                try
                {
                    rs = st.executeQuery("SELECT * FROM " + localBadTable + " WHERE " + localIdColumn + " ='" + i + "'");
                }
                catch (SQLException e)
                {
                    System.out.println(e.toString());
                    if (e.toString().contains("invalid memory alloc request size"))
                    {
                        long startDelete = new Date().getTime();
                        badRows++;
                        System.out.println("Bad row is '" + i + "' in " + localBadTable);
                        System.out.println("Try to delete bad row...");
                        PreparedStatement ps = null;
                        try
                        {
                            ps = db.prepareStatement("DELETE FROM " + localBadTable + " WHERE " + localIdColumn + " ='" + i + "'");
                            ps.executeUpdate();
                            ps.close();
                            long endDelete = new Date().getTime();
                            System.out.println("Bad row " + i + " in "+localBadTable+" was deleted in " + (endDelete - startDelete) + " ms");
                            deletedRows++;
                        }
                        catch (SQLException e1)
                        {
                            e1.printStackTrace();
                        }
                    }
                }
                if (i % 100 == 0)
                {
                    System.out.println("Checked " + i + " rows in " + localBadTable);
                }
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                    e.printStackTrace();
                }
            }
        }
        try
            {
                st.close();
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
            long end = new Date().getTime();
            long time = end - start;
            System.out.println("\nRepair " + localBadTable + " finished in " + String.format("%02d min and %02d sec",
                    TimeUnit.MILLISECONDS.toMinutes(time),
                    TimeUnit.MILLISECONDS.toSeconds(time) -
                            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time))
            ));
            System.out.println("Bad rows found :" + badRows);
            System.out.println("Bad rows deleted :" + deletedRows + "\n");
        }
}