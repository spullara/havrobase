/*
 * Copyright 2011 Jin Kwon.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package jinahya.rfc4648;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import jinahya.io.BitInput;
import jinahya.io.BitOutput;


/**
 *
 * @author <a href="mailto:jinahya@gmail.com">Jin Kwon</a>
 */
public abstract class Base {


    /** Default pad character. */
    protected static final char PAD = '=';


    /** MAGIC NUMBER: OCTET SIZE. */
    private static final int OCTET_SIZE = 8;


    /** MAGIC NUMBER: ASCII SIZE. */
    private static final int ASCII_SIZE = 128;


    /** MAGIC NUMBER: SMALLEST VISIBLE ASCII. */
    private static final int SMALLEST_VISIBLE_ASCII = 33;


    /**
     * Returns the Least Common Muliple value for given two operands.
     *
     * @param a the first operand
     * @param b the second operand
     * @return calculated least common multiple
     */
    private static int lcm(final int a, final int b) {
        return ((a * b) / gcd(a, b));
    }


    /**
     * Returns the Greatest Common Divisor for given two operands.
     *
     * @param a the first operand
     * @param b the second operand
     * @return calculated greate common devisor
     */
    private static int gcd(final int a, final int b) {
        if (b == 0) {
            return a;
        } else {
            return gcd(b, a % b);
        }
    }


    /**
     * Create a new instance.
     *
     * @param alphabet alphabe to be used
     * @param padding flag for padding
     */
    protected Base(final byte[] alphabet, final boolean padding) {
        super();

        if (alphabet == null) {
            throw new IllegalArgumentException("null alphabet");
        }

        if (alphabet.length == 0) {
            throw new IllegalArgumentException("empty alphabet");
        }
        
        encode = alphabet;

        decode = new byte[ASCII_SIZE - SMALLEST_VISIBLE_ASCII + 1];
        for (int i = 0; i < decode.length; i++) {
            decode[i] = -1;
        }
        for (byte i = 0; i < encode.length; i++) {
            decode[encode[i] - SMALLEST_VISIBLE_ASCII] = i;
        }

        this.padding = padding;

        bitsPerChar = (int) (Math.log(encode.length) / Math.log(2.0d));
        bytesPerWord = lcm(OCTET_SIZE, bitsPerChar) / OCTET_SIZE;
        charsPerWord = bytesPerWord * OCTET_SIZE / bitsPerChar;
    }


    /**
     *
     * @param input
     * @return
     * @throws IOException
     */
    public char[] encode(final byte[] input) throws IOException {

        if (input == null) {
            throw new IllegalArgumentException("null input");
        }

        return encode(new ByteArrayInputStream(input));
    }


    /**
     *
     * @param input
     * @return
     * @throws IOException
     */
    public char[] encode(final InputStream input) throws IOException {

        if (input == null) {
            throw new IllegalArgumentException("null input");
        }

        final CharArrayWriter output = new CharArrayWriter();

        encode(input, output);
        output.flush();

        return output.toCharArray();
    }


    /**
     *
     * @param input binary input
     * @param output character output
     * @throws IOException if an I/O error occurs
     */
    public final void encode(final InputStream input, final Writer output)
        throws IOException {

        if (input == null) {
            throw new IllegalArgumentException("null input");
        }

        if (output == null) {
            throw new IllegalArgumentException("null output");
        }

        encode(new BitInput(input), output);
    }


    /**
     *
     * @param input binary input
     * @param output character output
     * @throws IOException if an I/O error occurs
     */
    private void encode(final BitInput input, final Writer output)
        throws IOException {

        if (input == null) {
            throw new IllegalArgumentException("null input");
        }

        if (output == null) {
            throw new IllegalArgumentException("null output");
        }

        outer:
        while (true) {

            for (int i = 0; i < charsPerWord; i++) {

                int available = OCTET_SIZE - ((bitsPerChar * i) % OCTET_SIZE);

                if (available >= bitsPerChar) {
                    try {
                        int unsigned = input.readUnsignedInt(bitsPerChar);
                        output.write(encode[unsigned]);
                    } catch (EOFException eofe) { // i == 0
                        break outer;
                    }
                } else { // need next octet
                    int required = bitsPerChar - available;
                    int unsigned =
                        (input.readUnsignedInt(available) << required);
                    try {
                        unsigned |= input.readUnsignedInt(required);
                        output.write(encode[unsigned]);
                    } catch (EOFException eofe) {
                        output.write(encode[unsigned]);
                        if (padding) {
                            for (int j = i + 1; j < charsPerWord; j++) {
                                output.write(PAD);
                            }
                        }
                        break outer;
                    }
                }
            }
        }
    }


    /**
     *
     * @param input
     * @return
     * @throws IOException
     */
    public final byte[] decode(final char[] input) throws IOException {

        if (input == null) {
            throw new IllegalArgumentException("null input");
        }

        return decode(new CharArrayReader(input));
    }


    /**
     *
     * @param input
     * @return
     * @throws IOException
     */
    public final byte[] decode(final Reader input) throws IOException {

        if (input == null) {
            throw new IllegalArgumentException("null input");
        }

        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        decode(input, output);
        output.flush();

        return output.toByteArray();
    }


    /**
     *
     * @param input character input
     * @param output binary output
     * @throws IOException if I/O error occurs
     */
    public final void decode(final Reader input, final OutputStream output)
        throws IOException {

        if (input == null) {
            throw new IllegalArgumentException("null input");
        }

        if (output == null) {
            throw new IllegalArgumentException("null outpute");
        }

        decode(input, new BitOutput(output));
    }


    /**
     *
     * @param input character input
     * @param output binary output
     * @throws IOException if I/O error occurs
     */
    private void decode(final Reader input, final BitOutput output)
        throws IOException {

        outer:
        while (true) {

            int c;

            for (int i = 0; i < charsPerWord; i++) {

                c = input.read();

                if (c == -1) { // end of stream

                    if (i == 0) { // first character in a word; ok
                        break outer;
                    }

                    if (((i * bitsPerChar) % OCTET_SIZE) >= bitsPerChar) {
                        throw new EOFException("not finished properly");
                    }

                    if (!padding) {
                        break outer;
                    }

                    throw new EOFException("not finished properly");

                } else if (c == PAD) {

                    if (!padding) {
                        throw new IOException("bad padding; no pads allowed");
                    }

                    if (i == 0) { // first character in a word
                        throw new IOException("bad padding");
                    }

                    if (((i * bitsPerChar) % OCTET_SIZE) >= bitsPerChar) {
                        throw new IOException("bad padding");
                    }

                    for (int j = i + 1; j < charsPerWord; j++) {
                        c = input.read(); // pad
                        if (c == -1) { // end of stream?
                            throw new EOFException("not finished properly");
                        }
                        if (c != PAD) { // not the pad char?
                            throw new IOException("bad padding");
                        }
                    }

                    break outer;

                } else {

                    int value = decode[c - SMALLEST_VISIBLE_ASCII];
                    if (value == -1) {
                        throw new IOException("bad character: " + (char) c);
                    }
                    output.writeUnsignedInt(bitsPerChar, value);
                }
            }
        }
    }


    /** Characters for encoding. */
    private final byte[] encode;


    /** Characters for decoding. */
    private final byte[] decode;


    /** flag for padding. */
    private final boolean padding;


    /** number of bits per character. */
    private final int bitsPerChar;


    /** number of bytes per word. */
    private final int bytesPerWord;


    /** number of characters per word. */
    private final int charsPerWord;
}
