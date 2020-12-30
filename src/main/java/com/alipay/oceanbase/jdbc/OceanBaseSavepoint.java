package com.alipay.oceanbase.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;
import com.alipay.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

public class OceanBaseSavepoint implements Savepoint {

    private final String name;

    public OceanBaseSavepoint(final String name) {
        this.name = name;
    }

    /**
     * Retrieves the generated ID for the savepoint that this <code>Savepoint</code> object
     * represents.
     *
     * @return the numeric ID of this savepoint
     */
    public int getSavepointId() throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("Doesn't support savepoint identifier");
    }

    /**
     * Retrieves the name of the savepoint that this <code>Savepoint</code> object represents.
     *
     * @return the name of this savepoint
     */
    public String getSavepointName() {
        return name;
    }

    @Override
    public String toString() {
        return "MariaDbSavepoint{" + "name='" + name + '\'' + '}';
    }
}
