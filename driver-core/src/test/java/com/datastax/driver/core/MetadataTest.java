package com.datastax.driver.core;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

public class MetadataTest {

    List<String> schema = Lists.newArrayList(
        "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "USE test",
        "CREATE TABLE IF NOT EXISTS foo(i int primary key)",
        "INSERT INTO foo (i) VALUES (1)",
        "INSERT INTO foo (i) VALUES (2)",
        "INSERT INTO foo (i) VALUES (3)"
    );

    @Test(groups = "short")
    public void should_expose_token_ranges_for_murmur3_partitioner() throws Exception {
        should_expose_token_ranges();
    }

    @Test(groups = "short")
    public void should_expose_token_ranges_for_random_partitioner() throws Exception {
        should_expose_token_ranges("-p RandomPartitioner");
    }

    @Test(groups = "short")
    public void should_expose_token_ranges_for_ordered_partitioner() throws Exception {
        should_expose_token_ranges("-p ByteOrderedPartitioner");
    }

    private void should_expose_token_ranges(String... ccmOptions) throws Exception {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test", 3, ccmOptions);
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .build();
            Metadata metadata = cluster.getMetadata();
            Session session = cluster.connect();

            for (String statement : schema)
                session.execute(statement);

            // Find the replica for a given partition key
            int testKey = 1;
            Set<Host> replicas = metadata.getReplicas("test", DataType.cint().serialize(testKey));
            assertThat(replicas).hasSize(1);
            Host replica = replicas.iterator().next();

            // Iterate the cluster's token ranges. For each one, use a range query to ask Cassandra which partition keys
            // are in this range.
            Set<TokenRange> ranges = metadata.getTokenRanges();
            PreparedStatement between = session.prepare("SELECT i FROM foo WHERE token(i) > :start and token(i) <= :end");
            PreparedStatement after = session.prepare("SELECT i FROM foo WHERE token(i) > :start");
            PreparedStatement before = session.prepare("SELECT i FROM foo WHERE token(i) <= :end");
            TokenRange foundRange = null;
            for (TokenRange range : ranges) {
                Token start = range.getStart(), end = range.getEnd();
                List<Row> rows;
                if (end.isMinToken()) {
                    rows = session.execute(after.bind()
                        .setBytesUnsafe("start", start.serialize())).all();
                } else if (start.compareTo(end) < 0) {
                    rows = session.execute(between.bind()
                        .setBytesUnsafe("start", start.serialize())
                        .setBytesUnsafe("end", end.serialize())).all();
                } else {
                    rows = Lists.newArrayList();
                    rows.addAll(
                        session.execute(after.bind()
                            .setBytesUnsafe("start", start.serialize())).all()
                    );
                    rows.addAll(
                        session.execute(before.bind()
                            .setBytesUnsafe("end", end.serialize())).all()
                    );
                }
                for (Row row : rows) {
                    if (row.getInt("i") == testKey) {
                        // We should find our test key exactly once
                        assertThat(foundRange)
                            .describedAs("found the same key in two ranges: " + foundRange + " and " + range)
                            .isNull();
                        foundRange = range;
                        // That range should be managed by the replica
                        assertThat(metadata.getReplicas("test", range)).contains(replica);
                    }
                }
            }
            assertThat(foundRange).isNotNull();
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
}
