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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static java.util.Locale.ENGLISH;

public class OracleClient
        extends BaseJdbcClient
{
    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config, OracleConfig oracleConfig)
    {
        super(connectorId, config, "", connectionFactory(config, oracleConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig)
    {
        Properties connectionProperties = basicConnectionProperties(config);

        if (oracleConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(oracleConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(oracleConfig.getMaxReconnects()));
        }
        if (oracleConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(oracleConfig.getConnectionTimeout().toMillis()));
        }

        return new DriverConnectionFactory(new OracleDriver(), config.getConnectionUrl(), connectionProperties);
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = connectionFactory.openConnection();
                ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase(ENGLISH);
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        return connection.getMetaData().getTables(connection.getCatalog(), schemaName, tableName, new String[] {"TABLE", "SYNONYM"});
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        return new SchemaTableName(
                resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }

    @Override
    protected String toSqlType(Type type)
    {
        return super.toSqlType(type);
    }
}
