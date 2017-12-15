import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class DBOutputWritable implements DBWritable{

    private String starting_phase;
    private String following_word;
    private int count;

    public DBOutputWritable(String starting_phase, String following_word, int count) {
        this.starting_phase = starting_phase;
        this.following_word = following_word;
        this.count = count;
    }


    public void readFields(ResultSet resultSet) throws SQLException {
        starting_phase = resultSet.getString(1);
        following_word = resultSet.getString(2);
        count = resultSet.getInt(3);
    }

    public void write(PreparedStatement ps) throws SQLException {
        ps.setString(1, starting_phase);
        ps.setString(2, following_word);
        ps.setInt(3, count);
    }

}
