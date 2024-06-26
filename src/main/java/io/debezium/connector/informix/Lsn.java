/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import io.debezium.connector.Nullable;

/**
 * A logical representation of LSN (log sequence number) position. When LSN is not available
 * it is replaced with {@link Lsn#NULL} constant.
 *
 * @author Laoflch Luo, Xiaolin Zhang, Lars M Johansson
 *
 */
public class Lsn implements Comparable<Lsn>, Nullable {

    public static final Lsn NULL = new Lsn(-1L);

    private static final long LO_MASK = Long.parseUnsignedLong("00000000ffffffff", 16);
    private static final long HI_MASK = Long.parseUnsignedLong("ffffffff00000000", 16);

    private final Long sequence;

    Lsn(Long sequence) {
        this.sequence = sequence;
    }

    /**
     * Creates an LSN object from a logical log sequence number's string representation.
     *
     * @param sequence - signed long integer string. We consider "NULL" and "-1L" as same as "new Lsn(-1)".
     * @return LSN converted from its textual representation
     */
    public static Lsn of(String sequence) {
        return (sequence == null || sequence.equalsIgnoreCase("NULL")) ? NULL : Lsn.of(Long.parseLong(sequence));

    }

    /**
     * Creates an LSN object from a logical log sequence number.
     *
     * @param sequence logical sequence number
     * @return LSN representing the given sequence number
     */
    public static Lsn of(Long sequence) {
        return sequence == null ? NULL : new Lsn(sequence);
    }

    /**
     * Creates an LSN from a logical log file unique id and position within the log file.
     * The unique id is an integer that goes in the upper 32 bits of the 64 bit sequence number.
     * The log position is a 32 bit address within the log file and goes in the lower 32 bits of the sequence number.
     *
     * @param loguniq unique id of the logical log page
     * @param logpos position within the logical log page
     * @return LSN representing the given log file unique id and position within the log file
     */
    public static Lsn of(long loguniq, long logpos) {
        return Lsn.of((loguniq << 32) + logpos);
    }

    /**
     * @return true if this is a real LSN or false it it is {@code NULL}
     */
    @Override
    public boolean isAvailable() {
        return sequence != null && sequence >= 0;
    }

    /**
     * @return textual representation of the stored LSN
     */
    public String toString() {
        return Long.toString(sequence);
    }

    /**
     * Return the LSN String for an official representation, like "LSN(7,8a209c)". Reference
     * <a href="https://www.oninit.com/manual/informix/117/documentation/ids_cdc_bookmap.pdf">Informix 11.70 CDC API Programmer's Guide</a> page 62 for
     * more details.
     *
     * @return official textual representation of LSN.
     */
    public String toLongString() {
        return String.format("LSN(%d,%x)", loguniq(), logpos());
    }

    /** 32bit position within the current log page */
    public long logpos() {
        return LO_MASK & sequence;
    }

    /** 32bit log page unique identifier */
    public long loguniq() {
        return sequence >> 32;
    }

    public long sequence() {
        return sequence != null ? sequence : -1L;
    }

    @Override
    public int hashCode() {
        return sequence.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj != null && this.getClass().equals(obj.getClass()) && sequence.equals(((Lsn) obj).sequence);
    }

    /**
     * Enables ordering of LSNs. The {@code NULL} LSN is always the smallest one.
     */
    @Override
    public int compareTo(Lsn o) {
        if (this == o) {
            return 0;
        }
        if (!this.isAvailable()) {
            if (!o.isAvailable()) {
                return 0;
            }
            return -1;
        }
        if (!o.isAvailable()) {
            return 1;
        }
        return sequence.compareTo(o.sequence);
    }
}
