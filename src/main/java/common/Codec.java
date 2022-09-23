package common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nustaq.serialization.FSTConfiguration;

// https://github.com/RuedigerMoeller/fast-serialization/wiki/Serialization
// Make sure you read the link before you change this implementation.
// Refer test/common/CodingTest.java for more usage example.

// NOTE!: property key should not use encode/decodeValue(for graph structure locality)
// NOTE!: you can use encode/decodeValue to encode/decode log key.

// FST will try his best to compress object, thus will probably not maintain the consistency of the prefix.
// For example: k0: "crusher-k0" k1: "crusher-k1"
// k0 and k1 may not have the same byte array prefix.
// Basically, we use encode/decodeValue to encode/decode entity temporal property value.
// And we know Neo4j entity property key should always be String object.
// For temporal property key, we distinguish it by vertex and edge.
// For vertex:
//      key: NodeId(long) + String + timestamp(long)
// For edge:
//      key: StartNodeId(long) + EndNodeId(long) + String + timestamp(long)
// For vertex/edge temporal property key, you should custom your own prefix/range consistency Codec.
public class Codec {
    // We aim to be compatible with Neo4j, thus we only preregister the class that Neo4j supports.
    // https://neo4j.com/docs/java-reference/4.4/javadocs/org/neo4j/graphdb/Entity.html

    private static final Log log = LogFactory.getLog(Codec.class);

    private static final ThreadLocal<FSTConfiguration> conf = ThreadLocal.withInitial(() -> {
        FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
        // TODO(crusher): Maybe we can preregister class here to accelerate.
        conf.setShareReferences(false);
        return conf;
    });

    public static byte[] encodeValue(Object object) {
        return conf.get().asByteArray(object);
    }

    public static Object decodeValue(byte[] bytes) {
        return conf.get().asObject(bytes);
    }


}