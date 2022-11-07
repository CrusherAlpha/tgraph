package txn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LogWriteBatch implements Serializable {

    private final List<LogEntry> logs;

    public LogWriteBatch() {
        this.logs = new ArrayList<>();
    }


    public void append(LogEntry entry) {
        logs.add(entry);
    }

    public List<LogEntry> getLogs() {
        return logs;
    }

    public int size() {
        return logs.size();
    }

    @Override
    public String toString() {
        return "LogWriteBatch{" +
                "logs=" + logs +
                '}';
    }
}
