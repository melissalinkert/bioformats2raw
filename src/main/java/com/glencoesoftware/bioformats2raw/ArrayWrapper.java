/**
 * Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */

package com.glencoesoftware.bioformats2raw;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.bc.zarr.ZarrArray;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.v3.Array;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;

/**
 * Wraps a Zarr v2 (jzarr) or Zarr v3 (zarr-java) array.
 * This makes it a little easier for the converter to pass arrays
 * around without adding version checks everywhere in the main
 * conversion logic.
 */
public class ArrayWrapper {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(ArrayWrapper.class);

  private ZarrArray v2 = null;
  private Array v3 = null;

  /**
   * Create wrapper for v2 (jzarr) array.
   *
   * @param z jzarr array
   */
  public ArrayWrapper(ZarrArray z) {
    v2 = z;
  }

  /**
   * Create wrapper for v3 (zarr-java) array.
   *
   * @param z zarr-java array
   */
  public ArrayWrapper(Array z) {
    v3 = z;
  }

  /**
   * @return true if a v3 array is wrapped
   */
  public boolean isV3() {
    return v3 != null;
  }

  /**
   * @return int array representing the wrapped array's shape
   */
  public int[] getShape() {
    if (isV3()) {
      int[] shape = new int[v3.metadata.shape.length];
      for (int i=0; i<shape.length; i++) {
        shape[i] = (int) v3.metadata.shape[i];
      }
      return shape;
    }
    return v2.getShape();
  }

  /**
   * @return int array representing the wrapped array's chunk shape
   */
  public int[] getChunks() {
    if (isV3()) {
      return v3.metadata.chunkShape();
    }
    return v2.getChunks();
  }

  /**
   * @return UCAR data type for this array
   */
  public DataType getDataType() {
    if (isV3()) {
      return v3.metadata.dataType.getMA2DataType();
    }
    switch (v2.getDataType()) {
      case f8:
        return DataType.DOUBLE;
      case f4:
        return DataType.FLOAT;
      case i4:
        return DataType.INT;
      case u4:
        return DataType.UINT;
      case i2:
        return DataType.SHORT;
      case u2:
        return DataType.USHORT;
      case i1:
        return DataType.BYTE;
      case u1:
        return DataType.UBYTE;
      default:
        throw new IllegalArgumentException(
          "Unsupported v2 data type: " + v2.getDataType());
    }
  }

  /**
   * Read array contents as bytes into the given buffer array.
   *
   * @param buf buffer for array contents
   * @param shape shape of requested tile
   * @param offset offset of requested tile
   */
  public void read(byte[] buf, int[] shape, int[] offset)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      try {
        LOGGER.debug("reading shape {} from v3 array {}", shape, v3);
        long[] shapeV3 = getV3Shape(shape);
        ucar.ma2.Array array = v3.read(shapeV3, offset);
        LOGGER.debug("  requested shape: {}, returned shape: {}",
          shapeV3, array.getShape());
        ByteBuffer bb = array.getDataAsByteBuffer();
        bb.get(buf);
      }
      catch (ZarrException e) {
        throw new IOException(e);
      }
    }
    else {
      v2.read(buf, shape, offset);
    }
  }

  /**
   * Read array contents as shorts into the given buffer array.
   *
   * @param buf buffer for array contents
   * @param shape shape of requested tile
   * @param offset offset of requested tile
   */
  public void read(short[] buf, int[] shape, int[] offset)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      try {
        long[] shapeV3 = getV3Shape(shape);
        ucar.ma2.Array array = v3.read(shapeV3, offset);
        ByteBuffer bb = array.getDataAsByteBuffer();
        bb.asShortBuffer().get(buf);
      }
      catch (ZarrException e) {
        throw new IOException(e);
      }
    }
    else {
      v2.read(buf, shape, offset);
    }
  }

  /**
   * Read array contents as ints into the given buffer array.
   *
   * @param buf buffer for array contents
   * @param shape shape of requested tile
   * @param offset offset of requested tile
   */
  public void read(int[] buf, int[] shape, int[] offset)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      try {
        long[] shapeV3 = getV3Shape(shape);
        ucar.ma2.Array array = v3.read(shapeV3, offset);
        ByteBuffer bb = array.getDataAsByteBuffer();
        bb.asIntBuffer().get(buf);
      }
      catch (ZarrException e) {
        throw new IOException(e);
      }
    }
    else {
      v2.read(buf, shape, offset);
    }
  }

  /**
   * Read array contents as floats into the given buffer array.
   *
   * @param buf buffer for array contents
   * @param shape shape of requested tile
   * @param offset offset of requested tile
   */
  public void read(float[] buf, int[] shape, int[] offset)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      try {
        long[] shapeV3 = getV3Shape(shape);
        ucar.ma2.Array array = v3.read(shapeV3, offset);
        ByteBuffer bb = array.getDataAsByteBuffer();
        bb.asFloatBuffer().get(buf);
      }
      catch (ZarrException e) {
        throw new IOException(e);
      }
    }
    else {
      v2.read(buf, shape, offset);
    }
  }

  /**
   * Read array contents as doubles into the given buffer array.
   *
   * @param buf buffer for array contents
   * @param shape shape of requested tile
   * @param offset offset of requested tile
   */
  public void read(double[] buf, int[] shape, int[] offset)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      try {
        long[] shapeV3 = getV3Shape(shape);
        ucar.ma2.Array array = v3.read(shapeV3, offset);
        ByteBuffer bb = array.getDataAsByteBuffer();
        bb.asDoubleBuffer().get(buf);
      }
      catch (ZarrException e) {
        throw new IOException(e);
      }
    }
    else {
      v2.read(buf, shape, offset);
    }
  }

  /**
   * Write the given byte array to this array.
   *
   * @param buf data to be written
   * @param shape shape of the data
   * @param offset offset within the array at which to start writing
   * @param dataType UCAR data type (distinguish between signed/unsigned)
   */
  public void write(byte[] buf, int[] shape, int[] offset, DataType dataType)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      ucar.ma2.Array array = ucar.ma2.Array.factory(dataType, shape, buf);
      LOGGER.debug("writing v3 array {} to {}", array, v3);
      v3.write(getV3Shape(offset), array);
    }
    else {
      v2.write(buf, shape, offset);
    }
  }

  /**
   * Write the given short array to this array.
   *
   * @param buf data to be written
   * @param shape shape of the data
   * @param offset offset within the array at which to start writing
   * @param dataType UCAR data type (distinguish between signed/unsigned)
   */
  public void write(short[] buf, int[] shape, int[] offset, DataType dataType)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      ucar.ma2.Array array = ucar.ma2.Array.factory(dataType, shape, buf);
      v3.write(getV3Shape(offset), array);
    }
    else {
      v2.write(buf, shape, offset);
    }
  }

  /**
   * Write the given int array to this array.
   *
   * @param buf data to be written
   * @param shape shape of the data
   * @param offset offset within the array at which to start writing
   * @param dataType UCAR data type (distinguish between signed/unsigned)
   */
  public void write(int[] buf, int[] shape, int[] offset, DataType dataType)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      ucar.ma2.Array array = ucar.ma2.Array.factory(dataType, shape, buf);
      v3.write(getV3Shape(offset), array);
    }
    else {
      v2.write(buf, shape, offset);
    }
  }

  /**
   * Write the given float array to this array.
   *
   * @param buf data to be written
   * @param shape shape of the data
   * @param offset offset within the array at which to start writing
   * @param dataType UCAR data type (distinguish between signed/unsigned)
   */
  public void write(float[] buf, int[] shape, int[] offset, DataType dataType)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      ucar.ma2.Array array = ucar.ma2.Array.factory(dataType, shape, buf);
      v3.write(getV3Shape(offset), array);
    }
    else {
      v2.write(buf, shape, offset);
    }
  }

  /**
   * Write the given double array to this array.
   *
   * @param buf data to be written
   * @param shape shape of the data
   * @param offset offset within the array at which to start writing
   * @param dataType UCAR data type (distinguish between signed/unsigned)
   */
  public void write(double[] buf, int[] shape, int[] offset, DataType dataType)
    throws InvalidRangeException, IOException
  {
    if (isV3()) {
      ucar.ma2.Array array = ucar.ma2.Array.factory(dataType, shape, buf);
      v3.write(getV3Shape(offset), array);
    }
    else {
      v2.write(buf, shape, offset);
    }
  }

  private long[] getV3Shape(int[] shape) {
    long[] shapeV3 = new long[shape.length];
    for (int i=0; i<shape.length; i++) {
      shapeV3[i] = shape[i];
    }
    return shapeV3;
  }

}
