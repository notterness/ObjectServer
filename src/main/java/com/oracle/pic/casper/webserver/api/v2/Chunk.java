package com.oracle.pic.casper.webserver.api.v2;

import com.oracle.oci.casper.jopenssl.CryptoCipher;
import com.oracle.pic.casper.common.encryption.EncryptionKey;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

import java.util.Iterator;

public class Chunk {
    private final byte[] data;
    // beginning index of array, inclusive
    private final int beginIx;
    // end index of array, exclusive
    private final int endIx;

    private Chunk(byte[] data) {
        this(data, 0, data.length);
    }

    private Chunk(byte[] data, int beginIx, int endIx) {
        this.data = data;
        this.beginIx = beginIx;
        this.endIx = endIx;
    }

    public static Iterable<Chunk> getChunksFromStream(InputStream inputStream, int chunkSize) {
        return () -> new Iterator<Chunk>() {

            private boolean endOfStream = false;

            @Override
            public boolean hasNext() {
                return !endOfStream;
            }

            @Override
            public Chunk next() {
                byte[] buf = new byte[chunkSize];
                int ix = 0;
                while (ix < chunkSize) {
                    try {
                        int numRead = inputStream.read(buf, ix, chunkSize - ix);
                        if (numRead == -1) {
                            endOfStream = true;
                            break;
                        }
                        ix += numRead;
                    } catch (IOException e) {
                        throw new RuntimeException("blah");
                    }
                }
                return new Chunk(buf, 0, ix);
            }
        };
    }

    public Chunk encrypt(EncryptionKey encryptionKey) {
        CryptoCipher cipher;
        try {
            cipher = encryptionKey.dataCipher(javax.crypto.Cipher.ENCRYPT_MODE, 0, false);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Couldn't get cipher");
        }
        byte[] encryptedChunk = cipher.update(data, beginIx, endIx - beginIx);
        return new Chunk(encryptedChunk);
    }

    public int getSize() {
        return endIx - beginIx;
    }

    public Iterable<Chunk> getSubChunks(int subChunkSize) {
        return () -> new Iterator<Chunk> () {
            private int curIx = Chunk.this.beginIx;

            @Override
            public boolean hasNext() {
                return curIx < Chunk.this.endIx;
            }

            @Override
            public Chunk next() {
                int newEndIx = Math.min(curIx + subChunkSize, Chunk.this.endIx);
                Chunk subChunk = new Chunk(Chunk.this.data, curIx, newEndIx);
                curIx = newEndIx;
                return subChunk;
            }
        };
    }

    public byte[] getData() {
        byte[] dataCopy = new byte[endIx - beginIx];
        System.arraycopy(data, beginIx, dataCopy, 0, endIx - beginIx);
        return dataCopy;
    }
}
