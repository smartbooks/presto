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

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;

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
                String schemaName = resultSet.getString(1).toLowerCase();
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        // Here we put TABLE and SYNONYM when the table schema is another user schema
        return connection.getMetaData().getTables(null, schemaName, tableName, new String[]{"TABLE", "SYNONYM"});
    }

    @Override
    protected String toSqlType(Type type)
    {
        return super.toSqlType(type);
    }

    protected Type toPrestoType(int jdbcType, int columnSize, int decimalDigits)
    {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
                return TINYINT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.REAL:
                return REAL;
            case Types.NUMERIC:
                return createDecimalType(columnSize == 0 ? 38 : columnSize, max(decimalDigits, 0));
            case Types.FLOAT:
            case Types.DECIMAL:
            case Types.DOUBLE:
                return DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
                return createCharType(min(columnSize, CharType.MAX_LENGTH));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == 0) {
                    return createUnboundedVarcharType();
                }
                return createVarcharType(columnSize);
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VARBINARY;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
        }
        return null;
    }
}
