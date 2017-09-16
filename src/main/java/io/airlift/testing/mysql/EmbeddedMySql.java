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

import io.airlift.command.Command;
import io.airlift.command.CommandFailedException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.StandardSystemProperty.OS_ARCH;
import static com.google.common.base.StandardSystemProperty.OS_NAME;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.io.File.createTempFile;
import static java.lang.String.format;
import static java.nio.file.Files.copy;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

final class EmbeddedMySql
        implements Closeable
{
    private static final Logger log = Logger.get(EmbeddedMySql.class);

    private static final String JDBC_FORMAT = "jdbc:mysql://localhost:%s/%s?user=%s&useSSL=false";

    private static final Duration STARTUP_WAIT = new Duration(10, SECONDS);
    private static final Duration SHUTDOWN_WAIT = new Duration(10, SECONDS);
    private static final Duration COMMAND_TIMEOUT = new Duration(30, SECONDS);

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("testing-mysql-server-%s"));
    private final Path serverDirectory;
    private final int port = randomPort();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Process mysqld;

    public EmbeddedMySql()
            throws IOException
    {
        serverDirectory = createTempDirectory("testing-mysql-server");

        log.info("Starting MySQL server in %s", serverDirectory);

        try {
            unpackMySql(serverDirectory);
            initialize();
            mysqld = startMysqld();
        }
        catch (Exception e) {
            close();
            throw e;
        }
    }

    public String getJdbcUrl(String userName, String dbName)
    {
        return format(JDBC_FORMAT, port, dbName, userName);
    }

    public int getPort()
    {
        return port;
    }

    public Connection getMySqlDatabase()
            throws SQLException
    {
        return DriverManager.getConnection(getJdbcUrl("root", "mysql"));
    }

    @Override
    public void close()
    {
        if (closed.getAndSet(true)) {
            return;
        }

        if (mysqld != null) {
            log.info("Shutting down mysqld. Waiting up to %s for shutdown to finish.", STARTUP_WAIT);

            mysqld.destroyForcibly();

            try {
                mysqld.waitFor(SHUTDOWN_WAIT.toMillis(), MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (mysqld.isAlive()) {
                log.error("mysqld is still running in %s", serverDirectory);
            }
        }

        try {
            deleteRecursively(serverDirectory, ALLOW_INSECURE);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete %s", serverDirectory);
        }

        executor.shutdownNow();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("serverDirectory", serverDirectory)
                .add("port", port)
                .toString();
    }

    private static int randomPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void initialize()
    {
        system(mysqld(),
                "--no-defaults",
                "--initialize-insecure",
                "--datadir", dataDir());
    }

    private Process startMysqld()
            throws IOException
    {
        List<String> args = newArrayList(
                mysqld(),
                "--no-defaults",
                "--skip-ssl",
                "--disable-partition-engine-check",
                "--explicit_defaults_for_timestamp",
                "--lc_messages_dir", serverDirectory.resolve("share").toString(),
                "--socket", serverDirectory.resolve("mysql.sock").toString(),
                "--port", String.valueOf(port),
                "--datadir", dataDir());

        Process process = new ProcessBuilder(args)
                .redirectErrorStream(true)
                .redirectOutput(Redirect.INHERIT)
                .start();

        log.info("mysqld started on port %s. Waiting up to %s for startup to finish.", port, STARTUP_WAIT);

        waitForServerStartup(process);

        return process;
    }

    private String mysqld()
    {
        return serverDirectory.resolve("bin").resolve("mysqld").toString();
    }

    private String dataDir()
    {
        return serverDirectory.resolve("data").toString();
    }

    private void waitForServerStartup(Process process)
            throws IOException
    {
        Throwable lastCause = null;
        long start = System.nanoTime();
        while (Duration.nanosSince(start).compareTo(STARTUP_WAIT) <= 0) {
            try {
                checkReady();
                log.info("mysqld startup finished");
                return;
            }
            catch (SQLException e) {
                lastCause = e;
            }

            try {
                // check if process has exited
                int value = process.exitValue();
                throw new IOException(format("mysqld exited with value %s, check stdout for more detail", value));
            }
            catch (IllegalThreadStateException ignored) {
                // process is still running, loop and try again
            }

            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new IOException("mysqld failed to start after " + STARTUP_WAIT, lastCause);
    }

    private void checkReady()
            throws SQLException
    {
        try (Connection connection = getMySqlDatabase();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT 42")) {
            checkSql(resultSet.next(), "no rows in result set");
            checkSql(resultSet.getInt(1) == 42, "wrong result");
            checkSql(!resultSet.next(), "multiple rows in result set");
        }
    }

    private static void checkSql(boolean expression, String message)
            throws SQLException
    {
        if (!expression) {
            throw new SQLException(message);
        }
    }

    private void system(String... command)
    {
        try {
            new Command(command)
                    .setTimeLimit(COMMAND_TIMEOUT)
                    .execute(executor);
        }
        catch (CommandFailedException e) {
            throw new RuntimeException(e);
        }
    }

    private void unpackMySql(Path target)
            throws IOException
    {
        String archiveName = format("/mysql-%s.tar.gz", getPlatform());
        URL url = EmbeddedMySql.class.getResource(archiveName);
        if (url == null) {
            throw new RuntimeException("archive not found: " + archiveName);
        }

        File archive = createTempFile("mysql-", null);
        try {
            try (InputStream in = url.openStream()) {
                copy(in, archive.toPath(), REPLACE_EXISTING);
            }
            system("tar", "-xzf", archive.getPath(), "-C", target.toString());
        }
        finally {
            if (!archive.delete()) {
                log.warn("Failed to delete file %s", archive);
            }
        }
    }

    private static String getPlatform()
    {
        return (OS_NAME.value() + "-" + OS_ARCH.value()).replace(' ', '_');
    }
}
