package com.alipay.oceanbase.jdbc.internal.com.read.resultset;

public class UpdatableColumnDefinition extends ColumnDefinition {

    private final boolean canBeNull;
    private final boolean primary;
    private final boolean hasDefault;
    private final boolean generated;
    private final boolean autoIncrement;

    /**
     * Constructor of Column metadata.
     *
     * @param columnDefinition column meta information
     * @param canBeNull can column be null
     * @param hasDefault has default
     * @param generated is automatically generated
     * @param primary is column part of primary key
     * @param autoIncrement column has AUTO_INCREMENT attribute
     */
    public UpdatableColumnDefinition(ColumnDefinition columnDefinition, boolean canBeNull,
                                     boolean hasDefault, boolean generated, boolean primary,
                                     boolean autoIncrement) {
        super(columnDefinition);
        this.canBeNull = canBeNull;
        this.hasDefault = hasDefault;
        this.generated = generated;
        this.primary = primary;
        this.autoIncrement = autoIncrement;
    }

    public boolean canBeNull() {
        return canBeNull;
    }

    public boolean hasDefault() {
        return hasDefault;
    }

    public boolean isGenerated() {
        return generated;
    }

    public boolean isPrimary() {
        return primary;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }
}
