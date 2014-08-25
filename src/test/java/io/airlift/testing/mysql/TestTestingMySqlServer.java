/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.testing.mysql;

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTestingMySqlServer
{
    @Test
    public void testDatabase()
            throws Exception
    {
        try (TestingMySqlServer server = new TestingMySqlServer("testuser", "testpass", "db1", "db2")) {
            assertTrue(server.isRunning());
            assertTrue(server.isReadyForConnections());
            assertEquals(server.getMySqlVersion(), "5.5.9");
            assertEquals(server.getDatabases(), ImmutableSet.of("db1", "db2"));
            assertEquals(server.getUser(), "testuser");
            assertEquals(server.getPassword(), "testpass");
            assertEquals(server.getJdbcUrl().substring(0, 5), "jdbc:");
            assertEquals(server.getPort(), URI.create(server.getJdbcUrl().substring(5)).getPort());

            for (String database : server.getDatabases()) {
                try (Connection connection = DriverManager.getConnection(server.getJdbcUrl())) {
                    connection.setCatalog(database);
                    try (Statement statement = connection.createStatement()) {
                        statement.execute("CREATE TABLE test_table (c1 bigint PRIMARY KEY)");
                        statement.execute("INSERT INTO test_table (c1) VALUES (1)");
                        try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM test_table")) {
                            assertTrue(resultSet.next());
                            assertEquals(resultSet.getLong(1), 1L);
                            assertFalse(resultSet.next());
                        }
                    }
                }
            }
        }
    }
}
