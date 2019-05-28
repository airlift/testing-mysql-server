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

import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MySqlOptions
{
    private final Duration startupWait;
    private final Duration shutdownWait;
    private final Duration commandTimeout;

    private MySqlOptions(Duration startupWait, Duration shutdownWait, Duration commandTimeout)
    {
        this.startupWait = requireNonNull(startupWait, "startupWait is null");
        this.shutdownWait = requireNonNull(shutdownWait, "shutdownWait is null");
        this.commandTimeout = requireNonNull(commandTimeout, "commandTimeout is null");
    }

    public Duration getStartupWait()
    {
        return startupWait;
    }

    public Duration getShutdownWait()
    {
        return shutdownWait;
    }

    public Duration getCommandTimeout()
    {
        return commandTimeout;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Duration startupWait = new Duration(10, SECONDS);
        private Duration shutdownWait = new Duration(10, SECONDS);
        private Duration commandTimeout = new Duration(30, SECONDS);

        public Builder setStartupWait(Duration startupWait)
        {
            this.startupWait = requireNonNull(startupWait, "startupWait is null");
            return this;
        }

        public Builder setShutdownWait(Duration shutdownWait)
        {
            this.shutdownWait = requireNonNull(shutdownWait, "shutdownWait is null");
            return this;
        }

        public Builder setCommandTimeout(Duration commandTimeout)
        {
            this.commandTimeout = requireNonNull(commandTimeout, "commandTimeout is null");
            return this;
        }

        public MySqlOptions build()
        {
            return new MySqlOptions(startupWait, shutdownWait, commandTimeout);
        }
    }
}
