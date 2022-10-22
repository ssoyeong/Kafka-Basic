package data.mgmt.cdckafka.service.impl;

import data.mgmt.cdckafka.service.CDCListenerService;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
@RequiredArgsConstructor
public class CDCListenerServiceImpl implements CDCListenerService {
    @Override
    public String cdcStart(Integer taskId, boolean isInitial) {

        // Define the configuration for the Debezium Engine with MySQL connector...
        final Properties props = new Properties();
        props.setProperty("name", "engine");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        props.setProperty("offset.flush.interval.ms", "60000");
        /* begin connector properties */
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "testuser");
        props.setProperty("database.password", "testuser");
        props.setProperty("database.server.id", "85744");
        props.setProperty("database.server.name", "my-app-connector");
        props.setProperty("database.history",
                "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename",
                "/path/to/storage/dbhistory.dat");

        // Create the engine with this configuration ...

        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    System.out.println(record);
                }).build();


        // Run the engine asynchronously ...
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        // Do something else or wait for a signal or an event

        return null;
    }

    @Override
    public String cdcStop(Integer taskId) {
        return null;
    }

    @Override
    public String cdcStatus(Integer taskId) {
        return null;
    }

}
