package org.wkh.learningavro;

import example.avro.User;
import org.junit.Test;

public class UserSerDeInformalBenchmark {
    private final User user = User.newBuilder()
            .setFavoriteColor("red")
            .setFavoriteNumber(42)
            .setName("Warren")
            .build();

    private final UserSerDe serde = new UserSerDe();

    @Test
    public void timeSerde() throws Exception {
        int length = 0;
        long start = System.currentTimeMillis();

        for(int i = 0; i < 1000; i++) {
            byte[] bytes = serde.serialize(user);
            length = bytes.length;
            User user = serde.deserialize(bytes);
        }

        long end = System.currentTimeMillis();

        /* this works because we did it in 1000 times */

        System.out.println((end - start) + " microseconds per");

        System.out.println("Size serialized: " + length);
    }
}
