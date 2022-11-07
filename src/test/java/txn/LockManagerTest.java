package txn;

import impl.tgraphdb.TGraphConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LockManagerTest {
    private static final Log log = LogFactory.getLog(LockManagerTest.class);

    private final ConcurrentHashMap<Long, TransactionImpl> txnMap = new ConcurrentHashMap<>();

    private TransactionImpl startTxn(long txnID) {
        var txn = new TransactionImpl(txnID);
        txnMap.put(txnID, txn);
        return txn;
    }

    private void clear() {
        txnMap.clear();
    }

    private void releaseLock(LockManager lm, TransactionImpl txn, TemporalPropertyID tp) {
        txn.setState(TransactionState.COMMITTED);
        try {
            lm.mu.lock();
            assertTrue(lm.unlock(txn, tp));
        } finally {
            lm.mu.unlock();
        }
    }

    private void releaseLocks(LockManager lm, TransactionImpl txn, List<TemporalPropertyID> tps) {
        txn.setState(TransactionState.COMMITTED);
        try {
            lm.mu.lock();
            for (var tp : tps) {
                assertTrue(lm.unlock(txn, tp));
            }
        } finally {
            lm.mu.unlock();
        }
    }

    @Test
    void testSS() {
        clear();

        var lm = new LockManager(txnMap, false);

        var txn1 = startTxn(1);
        var txn2 = startTxn(2);

        long vertexID = 1;
        String tpName = "lock-test";

        var tp = TemporalPropertyID.vertex(vertexID, tpName);

        var t1 = new Thread(() -> {
            try {
                assertTrue(lm.acquireShared(txn1, tp));
            } catch (TransactionAbortException e) {
                e.printStackTrace();
            }
        });

        var t2 = new Thread(() -> {
            try {
                assertTrue(lm.acquireShared(txn2, tp));
            } catch (TransactionAbortException e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testSX() {
        clear();

        var lm = new LockManager(txnMap, false);

        var txn1 = startTxn(1);
        var txn2 = startTxn(2);

        long vertexID = 1;
        String tpName = "lock-test";
        var tp = TemporalPropertyID.vertex(vertexID, tpName);

        var t1 = new Thread(() -> {
            try {
                assertTrue(lm.acquireShared(txn1, tp));
                TimeUnit.SECONDS.sleep(5);
                releaseLock(lm, txn1, tp);
            } catch (TransactionAbortException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        var t2 = new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                Instant start = Instant.now();
                assertTrue(lm.acquireExclusive(txn2, tp));
                Instant finish = Instant.now();
                long elapsed = Duration.between(start, finish).getSeconds();
                log.info(String.format("elapsed time: %d.", elapsed));
                assertTrue(elapsed > 2);
            } catch (TransactionAbortException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testConcurrentWrite() {
        clear();

        var lm = new LockManager(txnMap, false);

        long vertexID = 1;
        String tpName = "lock-test";
        var tp = TemporalPropertyID.vertex(vertexID, tpName);
        int[] tpValue = {0};

        long startTxnID = 1;

        // start three read transactions, tpValue should be 0.
        List<TransactionImpl> txns = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            txns.add(startTxn(startTxnID));
            ++startTxnID;
        }
        List<Thread> ths = new ArrayList<>();
        for (var txn : txns) {
            ths.add(new Thread(() -> {
                try {
                    assertTrue(lm.acquireShared(txn, tp));
                    assertEquals(0, tpValue[0]);
                    releaseLock(lm, txn, tp);
                } catch (TransactionAbortException e) {
                    e.printStackTrace();
                }
            }));
        }
        for (var th : ths) {
            th.start();
        }
        for (var th : ths) {
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // start ten write transactions
        txns.clear();
        ths.clear();
        for (int i = 0; i < 10; ++i) {
            txns.add(startTxn(startTxnID));
            ++startTxnID;
        }
        for (var txn : txns) {
            ths.add(new Thread(() -> {
                try {
                    lm.acquireExclusive(txn, tp);
                    ++tpValue[0];
                    releaseLock(lm, txn, tp);
                } catch (TransactionAbortException e) {
                    e.printStackTrace();
                }
            }));
        }

        for (var th : ths) {
            th.start();
        }
        for (var th : ths) {
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        assertEquals(10, tpValue[0]);
    }

    @Test
    void testMoreConflictOnSingleTp() {
        clear();

        var lm = new LockManager(txnMap, false);

        long vertexID = 1;
        String tpName = "lock-test";
        var tp = TemporalPropertyID.vertex(vertexID, tpName);
        int[] tpValue = {0};

        List<Thread> ths = new ArrayList<>();
        long txnID = 1;
        // 10 writers, 10 readers.
        for (int i = 0; i < 20; ++i) {
            var txn = startTxn(txnID + i);
            if (i % 2 == 0) {
                // reader
                ths.add(new Thread(() -> {
                    try {
                        assertTrue(lm.acquireShared(txn, tp));
                        releaseLock(lm, txn, tp);
                    } catch (TransactionAbortException e) {
                        e.printStackTrace();
                    }
                }));
            } else {
                // writer
                ths.add(new Thread(() -> {
                    try {
                        assertTrue(lm.acquireExclusive(txn, tp));
                        ++tpValue[0];
                        releaseLock(lm, txn, tp);
                    } catch (TransactionAbortException e) {
                        e.printStackTrace();
                    }
                }));
            }
        }

        for (var th : ths) {
            th.start();
        }

        for (var th : ths) {
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        assertEquals(10, tpValue[0]);
    }

    @Test
    void testLockUpgradeNormal() {
        clear();

        var lm = new LockManager(txnMap, false);

        var txn1 = startTxn(1);
        var txn2 = startTxn(2);
        var txn3 = startTxn(3);

        long vertexID = 1;
        String tpName = "lock-test";
        var tp = TemporalPropertyID.vertex(vertexID, tpName);


        // only one transaction, will succeed.
        var t1 = new Thread(() -> {
            try {
                assertTrue(lm.acquireShared(txn1, tp));
                assertTrue(lm.upgrade(txn1, tp));
                releaseLock(lm, txn1, tp);
            } catch (TransactionAbortException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        try {
            t1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // acquire S first, upgrade it then release it.
        var t2 = new Thread(() -> {
            try {
                assertTrue(lm.acquireShared(txn2, tp));
                assertTrue(lm.upgrade(txn2, tp));
                TimeUnit.SECONDS.sleep(3);
                releaseLock(lm, txn2, tp);
            } catch (TransactionAbortException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        // acquire X concurrently, will fail until txn2 release lock.
        var t3 = new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
                Instant start = Instant.now();
                assertTrue(lm.acquireExclusive(txn3, tp));
                Instant finish = Instant.now();
                long elapsed = Duration.between(start, finish).getSeconds();
                assertTrue(elapsed > 1);
            } catch (TransactionAbortException | InterruptedException e) {
                e.printStackTrace();
            }
        });


        t2.start();
        t3.start();
        try {
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    void testLockUpgradeConflict() {
        clear();

        var lm = new LockManager(txnMap, false);

        var txn1 = startTxn(1);
        var txn2 = startTxn(2);

        long vertexID = 1;
        String tpName = "lock-test";

        var tp = TemporalPropertyID.vertex(vertexID, tpName);

        // txn1, 2 acquire S-Lock first, then txn1 upgrade to X-Lock and txn2's upgrade will fail.
        var t1 = new Thread(() -> {
            try {
                assertTrue(lm.acquireShared(txn1, tp));
                // txn2 hold S-Lock on tp, thus should wait for it.
                // when txn2 failed for upgrade conflict, txn1 will upgrade successfully.
                assertTrue(lm.upgrade(txn1, tp));
            } catch (TransactionAbortException e) {
                e.printStackTrace();
            }
        });

        var t2 = new Thread(() -> {
            try {
                assertTrue(lm.acquireShared(txn2, tp));
                TimeUnit.SECONDS.sleep(1);
                lm.upgrade(txn2, tp);
            } catch (TransactionAbortException e) {
                assertEquals(AbortReason.UPGRADE_CONFLICT, e.getAbortReason());
                // NOTE!: when upgrade conflict occurs, caller should guarantee release all locks.
                releaseLock(lm, txn2, tp);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }



    @Test
    void testDeadlockDetectionBasic() {
        clear();

        var txn1 = startTxn(1);
        var txn2 = startTxn(2);
        var txn3 = startTxn(3);

        TGraphConfig.DEADLOCK_DETECT_INTERVAL = 15;

        var lm = new LockManager(txnMap, true);


        long vertexID = 1;
        String tpName = "lock-test";

        var tp1 = TemporalPropertyID.vertex(vertexID, tpName);

        var tp2 = TemporalPropertyID.vertex(vertexID + 1, tpName);

        var tp3 = TemporalPropertyID.vertex(vertexID + 2, tpName);

        var t1 = new Thread(() -> {
            try {
                assertTrue(lm.acquireExclusive(txn1, tp1));
                log.info("txn1 acquired tp1 succeeded");
                TimeUnit.SECONDS.sleep(1);
                log.info("txn1 finished sleeping");
                assertTrue(lm.acquireExclusive(txn1, tp2));
                ArrayList<TemporalPropertyID> tps = new ArrayList<>();
                tps.add(tp1);
                tps.add(tp2);
                releaseLocks(lm, txn1, tps);
            } catch (TransactionAbortException e) {
                assertEquals(AbortReason.DEADLOCK, e.getAbortReason());
                log.info("deadlock occurred.");
                releaseLock(lm, txn1, tp1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        var t2 = new Thread(() -> {
            try {
                assertTrue(lm.acquireExclusive(txn2, tp2));
                log.info("txn2 acquired tp2 succeeded");
                TimeUnit.SECONDS.sleep(1);
                log.info("txn2 finished sleeping");
                assertTrue(lm.acquireExclusive(txn2, tp3));
                ArrayList<TemporalPropertyID> tps = new ArrayList<>();
                tps.add(tp2);
                tps.add(tp3);
                releaseLocks(lm, txn2, tps);
            } catch (TransactionAbortException e) {
                assertEquals(AbortReason.DEADLOCK, e.getAbortReason());
                log.info("deadlock occurred.");
                releaseLock(lm, txn2, tp2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        var t3 = new Thread(() -> {
            try {
                assertTrue(lm.acquireExclusive(txn3, tp3));
                log.info("txn3 acquired tp3 succeeded");
                TimeUnit.SECONDS.sleep(1);
                log.info("txn3 finished sleeping");
                assertTrue(lm.acquireExclusive(txn3, tp1));
                ArrayList<TemporalPropertyID> tps = new ArrayList<>();
                tps.add(tp3);
                tps.add(tp1);
                releaseLocks(lm, txn3, tps);
            } catch (TransactionAbortException e) {
                assertEquals(AbortReason.DEADLOCK, e.getAbortReason());
                log.info("deadlock occurred.");
                releaseLock(lm, txn3, tp3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        t3.start();
        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
