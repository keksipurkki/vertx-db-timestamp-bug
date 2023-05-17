package net.keksipurkki.vertx;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class Repository {

    private final SqlClient client;

    Repository(SqlClient client) {
        this.client = client;
    }

    public Future<Void> createTable() {
        var sql = """
                        
            create table my_table(
                yolo text NOT NULL,
                created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT '2020-01-01'
            );
                        
            insert into my_table(yolo) values('YOLO');
                        
            """;
        return client.query(sql).execute().mapEmpty();
    }

    public Future<Instant> manifestsABug() {
        var row = client.query("select * from my_table")
            .mapping(Row::toJson)
            .execute()
            .map(this::toStream)
            .map(this.findFirstOrThrow(new DataAccessException("Yolo!?")));

        row.onSuccess(json -> {
            log.info("Timestamp string was {}", json.getString("created"));
        });

        return row.map(json -> Instant.parse(json.getString("created")));
    }

    private <T> Stream<T> toStream(RowSet<T> input) {
        return StreamSupport.stream(input.spliterator(), false);
    }

    private <T> Function<Stream<T>, T> findFirstOrThrow(DataAccessException exception) {
        return s -> s.findFirst().orElseThrow(() -> exception);
    }

}
