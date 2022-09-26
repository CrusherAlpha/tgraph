package impl.dbms;

import api.dbms.DatabaseManagementService;
import api.tgraphdb.TGraphDatabaseService;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.api.DatabaseNotFoundException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

// NOTE!: dbms is not thread safe
public class DatabaseManager implements DatabaseManagementService {

    private final MetaDB metaDB;

    private final org.neo4j.dbms.api.DatabaseManagementService neo4jDBMS;

    private final String metaPath;
    private final String tGraphPath;

    private HashMap<String, TGraphDatabaseService> opened;

    public DatabaseManager(String databaseDirectory) {
        this.metaPath = databaseDirectory + "/meta";
        this.tGraphPath = databaseDirectory + "/tgraph";
        this.metaDB = new MetaDB(this.metaPath);
        this.neo4jDBMS = new DatabaseManagementServiceBuilder(Path.of(this.tGraphPath)).build();
    }

    @Override
    public TGraphDatabaseService database(String databaseName) throws DatabaseNotFoundException {
        if (metaDB.get(databaseName) == null) {
            throw new DatabaseNotFoundException();
        }
        // TODO(crusher): return TGraphDatabaseService.
        return null;
    }

    @Override
    public void createDatabase(String databaseName) throws DatabaseExistsException {
        if (metaDB.get(databaseName) != null) {
            throw new DatabaseExistsException();
        }
        // TODO(crusher): create database
    }

    @Override
    public void dropDatabase(String databaseName) throws DatabaseNotFoundException {
        if (metaDB.get(databaseName) == null) {
            throw new DatabaseNotFoundException();
        }
        // TODO(crusher): drop database
    }

    @Override
    public void startDatabase(String databaseName) throws DatabaseNotFoundException {
        if (metaDB.get(databaseName) == null) {
            throw new DatabaseNotFoundException();
        }
        // TODO(crusher): start the database
    }

    @Override
    public void shutdownDatabase(String databaseName) throws DatabaseNotFoundException {
        if (metaDB.get(databaseName) == null) {
            throw new DatabaseNotFoundException();
        }
    }

    @Override
    public List<String> listDatabases() {
        var all = metaDB.scan();
        List<String> ret = new ArrayList<>();
        for (var pr : all) {
            ret.add(pr.first());
        }
        return ret;
    }

    @Override
    public void shutdown() {
        neo4jDBMS.shutdown();
        // TODO(crusher): shutdown temporal store
    }
}
