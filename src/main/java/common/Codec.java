package common;

import org.nustaq.serialization.FSTConfiguration;

// https://github.com/RuedigerMoeller/fast-serialization/wiki/Serialization
// Make sure you read the link before you change this implementation.
// Refer test/common/CodingTest.java for more usage example.

public class Codec {
    // We aim to be compatible with Neo4j, thus we only preregister the class that Neo4j supports.
    // https://neo4j.com/docs/java-reference/4.4/javadocs/org/neo4j/graphdb/Entity.html

    private static final ThreadLocal<FSTConfiguration> conf = ThreadLocal.withInitial(() -> {
        FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
        // TODO(crusher): Maybe we can preregister class here to accelerate.
        conf.setShareReferences(false);
        return conf;
    });

    public static byte[] encode(Object object) {
        return conf.get().asByteArray(object);
    }

    public static Object decode(byte[] bytes) {
        return conf.get().asObject(bytes);
    }
}