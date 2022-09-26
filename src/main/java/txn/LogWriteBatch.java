package txn;

import common.Codec;

import java.util.ArrayList;
import java.util.List;

public class LogWriteBatch {

    private final List<LogEntry> logs;

    public LogWriteBatch() {
        this.logs = new ArrayList<>();
    }


    void append(LogEntry entry) {
        logs.add(entry);
    }

    public byte[] toBytes() {
        return Codec.encodeValue(logs);
    }

}
