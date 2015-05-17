package org.wkh.learningavro;

import example.avro.User;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UserSerDeTest {
    @Test
    public void testSerializationDeserialization() throws Exception {
        User user = User.newBuilder()
                .setFavoriteColor("red")
                .setFavoriteNumber(42)
                .setName("Warren")
                .build();

        UserSerDe serde = new UserSerDe();

        byte[] serialized = serde.serialize(user);

        User newUser = serde.deserialize(serialized);

        assertEquals(newUser, user);
    }
}
