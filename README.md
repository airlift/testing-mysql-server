# Testing MySQL Server
[![Maven Central](https://img.shields.io/maven-central/v/io.airlift/testing-mysql-server.svg?label=Maven%20Central)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.airlift%22%20AND%20a%3A%22testing-mysql-server%22)
[![Build Status](https://travis-ci.org/airlift/testing-mysql-server.svg?branch=master)](https://travis-ci.org/airlift/testing-mysql-server)

Embedded MySQL server for use in tests. It allows testing your Java or
other JVM based application against a real MySQL server with no external
dependencies to deploy or manage.

## Usage

Add the library as a test dependency:

```xml
<dependency>
    <groupId>io.airlift</groupId>
    <artifactId>testing-mysql-server</artifactId>
    <version>5.7.19-1</version>
    <scope>test</scope>
</dependency>
```

Use it in your tests:

```java
@Test
public void testDatabase()
        throws Exception
{
    try (TestingMySqlServer server = new TestingMySqlServer("testuser", "testpass", "testdb");
            Connection connection = DriverManager.getConnection(server.getJdbcUrl("testdb"));
            Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE test_table (id bigint PRIMARY KEY)");
        statement.execute("INSERT INTO test_table (id) VALUES (123)");
    }
}

```

The server takes a few seconds to startup, so you will likely want to leave
it running between tests. Make sure the server is always shutdown (by calling
the `close()` method or using try-with-resources), otherwise the `mysqld`
process will stay running after the JVM exits.
 
## License

The code in this project is licensed under the [Apache License, Version 2.0](LICENSE).
The bundled MySQL server is licensed under the
[GNU General Public License (GPL), Version 2.0](https://downloads.mysql.com/docs/licenses/mysqld-5.7-gpl-en.pdf).
