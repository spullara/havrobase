/*
 *  Copyright 2010 Jin Kwon.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package jinahya.io;


import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;


/**
 *
 * @author <a href="mailto:jinahya@gmail.com">Jin Kwon</a>
 */
public class BitInput {


    /**
     * Creates a new instance.
     *
     * @param in source octet input
     */
    public BitInput(final InputStream in) {
        super();

        if (in == null) {
            throw new IllegalArgumentException("null in");
        }

        this.in = in;
    }


    /**
     * Reads an unsigned byte value.
     *
     * @param length bit length between 0 (exclusive) and 8 (inclusive)
     * @return an unsigned byte value.
     * @throws IOException if an I/O error occurs.
     */
    private int readUnsignedByte(final int length) throws IOException {

        if (length <= 0) {
            throw new IllegalArgumentException(
                "illegal length: " + length + " <= 0");
        }

        if (length > 8) {
            throw new IllegalArgumentException(
                "illegal length: " + length + " > 8");
        }

        if (index == 8) {
            int octet = in.read();
            if (octet == -1) {
                throw new EOFException("eof");
            }
            for (int i = 7; i >= 0; i--) {
                set.set(i, (octet & 0x01) == 0x01);
                octet >>= 1;
            }
            index = 0;
            count++;
        }

        final int required = length - (8 - index);
        if (required > 0) {
            return (readUnsignedByte(length - required) << required)
                   | readUnsignedByte(required);
        }

        int value = 0x00;
        for (int i = 0; i < length; i++) {
            value <<= 1;
            value |= (set.get(index + i) ? 0x01 : 0x00);
        }

        index += length;

        return value;
    }


    /**
     * Reads 1-bit long boolean value.
     *
     * @return boolean value
     * @throws IOException if an I/O error occurs.
     */
    public final boolean readBoolean() throws IOException {

        return readUnsignedByte(1) == 0x01;
    }


    /**
     * Reads an unsigned short value.
     *
     * @param length bit length between 0 (exclusive) and 16 (inclusive).
     * @return an unsigne short value.
     * @throws IOException if an I/O error occurs.
     */
    private int readUnsignedShort(final int length) throws IOException {

        if (length <= 0) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") <= 0");
        }

        if (length > 16) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") > 16");
        }

        final int quotient = length / 0x08;
        final int remainder = length % 0x08;

        int value = 0x00;
        for (int i = 0; i < quotient; i++) {
            value <<= 0x08;
            value |= readUnsignedByte(0x08);
        }

        if (remainder > 0) {
            value <<= remainder;
            value |= readUnsignedByte(remainder);
        }

        return value;
    }


    /**
     * Reads an unsigned int.
     *
     * @param length bit length between 1 (inclusive) and 32 (exclusive).
     * @return
     * @throws IOException
     */
    public int readUnsignedInt(final int length) throws IOException {

        if (length < 1) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") < 1");
        }

        if (length >= 32) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") >= 32");
        }

        final int quotient = length / 0x10;
        final int remainder = length % 0x10;

        int value = 0x00;
        for (int i = 0; i < quotient; i++) {
            value <<= 0x10;
            value |= readUnsignedShort(0x10);
        }

        if (remainder > 0) {
            value <<= remainder;
            value |= readUnsignedShort(remainder);
        }

        return value;
    }


    /**
     * Reads an signed int.
     *
     * @param length bit length between 1 (exclusive) and 32 (inclusive).
     * @return a <code>length</code>bit-long signed integer
     * @throws IOException if an I/O error occurs.
     */
    public int readInt(final int length) throws IOException {

        if (length <= 1) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") <= 1");
        }

        if (length > 32) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") > 32");
        }


        int value = readBoolean() ? (-1 << (length - 1)) : 0;

        value |= readUnsignedInt(length - 1);

        return value;
    }


    /**
     *
     * @param length bit length between 1 (inclusive) and 64 (exclusive)
     * @return <code>length</code>-bit unsigned long value
     * @throws IOException if an I/O error occurs
     */
    public final long readUnsignedLong(final int length) throws IOException {

        if (length < 1) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") < 1");
        }

        if (length >= 64) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") >= 64");
        }

        final int quotient = length / 0x10;
        final int remainder = length % 0x10;

        long value = 0x00L;
        for (int i = 0x00; i < quotient; i++) {
            value <<= 0x10;
            value |= readUnsignedShort(0x10);
        }

        if (remainder > 0x00) {
            value <<= remainder;
            value |= readUnsignedShort(remainder);
        }

        return value;
    }


    /**
     * Reads a signed long value.
     *
     * @param length bit length between 1 (exclusive) and 64 (inclusive).
     * @return a signed long value.
     * @throws IOException if an I/O error occurs.
     */
    public final long readLong(final int length) throws IOException {

        if (length <= 1) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") <= 1");
        }

        if (length > 64) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") > 64");
        }

        long value = readBoolean() ? (-1L << (length - 1)) : 0L;

        value |= readUnsignedLong(length - 1);

        return value;
    }


    /**
     * Align to given <code>length</code> bytes.
     *
     * @param length number of octets to align
     * @throws IOException if an I/O error occurs.
     */
    public void aling(final int length) throws IOException {

        if (length <= 0) {
            throw new IllegalArgumentException(
                "illegal length(" + length + ") <= 0");
        }

        int octets = count % length;

        if (octets > 0) {
            octets = length - octets;
        } else if (octets == 0) {
            octets = length;
        } else { // mod < 0
            octets = 0 - octets;
        }

        if (index < 8) {
            readUnsignedByte(8 - index);
            octets--;
        }

        if (octets == length) {
            return;
        }

        for (int i = 0; i < octets; i++) {
            readUnsignedByte(8);
        }
    }


    /**
     * Reads a sequence of bytes.
     *
     * @param bytes byte array
     * @throws IOException if an I/O error occurs.
     */
    public void readBytes(final byte[] bytes) throws IOException {
        readBytes(bytes, 0, bytes.length);
    }


    /**
     * Reads a sequence of bytes.
     *
     * @param bytes byte array
     * @param offset offset in bytes
     * @param length byte count to read
     * @throws IOException if an I/O error occurs.
     */
    public void readBytes(final byte[] bytes, final int offset,
                          final int length)
        throws IOException {

        if (bytes == null) {
            throw new IllegalArgumentException("null bytes");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("negative offset");
        }

        if (offset >= bytes.length) {
            throw new IllegalArgumentException(
                "offset(" + offset + ") >= bytes.length(" + bytes.length + ")");
        }

        if (length < 0) {
            throw new IllegalArgumentException("length(" + length + ") < 0");
        }

        if ((offset + length) > bytes.length) {
            throw new IllegalArgumentException(
                "offset(" + offset + ") + length(" + length
                + ") > bytes.length(" + bytes.length + ")");
        }

        for (int i = 0; i < length; i++) {
            bytes[offset + i] = (byte) readUnsignedByte(0x08);
        }
    }


    /** bit set. */
    private final BitSet set = new BitSet(8);


    /** input source. */
    private final InputStream in;


    /** bit index to read. */
    private int index = 8;


    /** octet count. */
    private int count = 0;
}
