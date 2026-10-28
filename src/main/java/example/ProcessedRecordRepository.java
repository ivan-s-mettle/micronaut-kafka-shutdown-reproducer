package example;

import jakarta.inject.Singleton;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Singleton
public class ProcessedRecordRepository {

    private final DataSource dataSource;

    public ProcessedRecordRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void save(long recordOffset) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "insert into processed_record (record_offset) values (?) on conflict do nothing")) {
            statement.setLong(1, recordOffset);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
