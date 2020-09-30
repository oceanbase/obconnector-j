package org.mariadb.jdbc.internal.com.send.parameters;

import java.io.IOException;
import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;

/** @Author : wb-xsy @Description @Date : 2020/9/27 18:56 */
public class OBBINARY_FLOATParameter implements Cloneable, ParameterHolder {
  private final float value;

  public OBBINARY_FLOATParameter(float value) {
    this.value = value;
  }

  @Override
  public void writeTo(PacketOutputStream os) throws IOException {
    os.write(String.valueOf(value));
  }

  @Override
  public void writeBinary(PacketOutputStream pos) throws IOException {
    pos.write(String.valueOf(value).getBytes());
  }

  @Override
  public int getApproximateTextProtocolLength() throws IOException {
    return String.valueOf(value).getBytes().length;
  }

  @Override
  public boolean isNullData() {
    return false;
  }

  @Override
  public ColumnType getColumnType() {
    return ColumnType.BINARY_FLOAT;
  }

  @Override
  public boolean isLongData() {
    return false;
  }

  public float getValue() {
    return value;
  }
}
