package com.boonya.bigdata.flink.kafka.hdfs;

import com.boonya.bigdata.flink.kafka.hdfs.model.UserEvent;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UserEventTest {

    @Test
    void shouldSerializeToJson() {
        UserEvent event = new UserEvent(1, "/home", 99.9, 1700000000000L, null);
        String json = event.toJson();
        assertTrue(json.contains("\"userId\":1"));
        assertTrue(json.contains("\"page\":\"/home\""));
    }

    @Test
    void shouldDeserializeFromJson() {
        String json = "{\"userId\":1,\"page\":\"/home\",\"amount\":99.9,\"timestamp\":1700000000000}";
        UserEvent event = UserEvent.fromJson(json);
        assertEquals(1, event.userId());
        assertEquals("/home", event.page());
        assertEquals(99.9, event.amount());
        assertEquals(1700000000000L, event.timestamp());
    }

    @Test
    void shouldHandleNullSalt() {
        UserEvent event = new UserEvent(1, "/home", 99.9, 1700000000000L, null);
        assertEquals("1", event.getSaltedKey());
    }

    @Test
    void shouldGenerateSaltedKey() {
        UserEvent event = new UserEvent(1, "/home", 99.9, 1700000000000L, null);
        UserEvent salted = event.withSalt(5);
        assertEquals("1_5", salted.getSaltedKey());
    }

    @Test
    void shouldSerializeToCsv() {
        UserEvent event = new UserEvent(1, "/home", 99.9, 1700000000000L, null);
        String csv = event.toCsv();
        assertEquals("1,/home,99.9,1700000000000,", csv);
    }

    @Test
    void shouldRejectInvalidJson() {
        assertThrows(Exception.class, () -> UserEvent.fromJson("{invalid}"));
    }
}
