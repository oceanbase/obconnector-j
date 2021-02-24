package com.alipay.oceanbase.jdbc.extend.datatype;

public class RowObCursorData {
    /**
     * Constructor.
     *
     * @param cursorId the cursor id
     * @param isOpen   the is open
     */
    public RowObCursorData(int cursorId, boolean isOpen) {
        this.cursorId = cursorId;
        this.isOpen = isOpen;
    }

    /**
     * Gets get cursor id.
     *
     * @return the get cursor id
     */
    public int getCursorId() {
        return cursorId;
    }

    /**
     * Sets set cursor id.
     *
     * @param cursorId the cursor id
     */
    public void setCursorId(int cursorId) {
        this.cursorId = cursorId;
    }

    private int cursorId;

    /**
     * Is open boolean.
     *
     * @return the boolean
     */
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Sets set open.
     *
     * @param open the open
     */
    public void setOpen(boolean open) {
        isOpen = open;
    }

    private boolean isOpen;
}
