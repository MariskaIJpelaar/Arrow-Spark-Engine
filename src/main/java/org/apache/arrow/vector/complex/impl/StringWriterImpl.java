package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.StringVector;
import org.apache.arrow.vector.types.pojo.Field;

public class StringWriterImpl extends AbstractFieldWriter
{
    final StringVector vector;

    public StringWriterImpl(StringVector vector)
    {
        this.vector = vector;
    }
    @Override
    public void allocate()
    {
        this.vector.allocateNew();
    }

    @Override
    public void clear()
    {
        this.vector.clear();
    }

    @Override
    public void close()
    {
        this.vector.close();
    }

    @Override
    public Field getField()
    {
        return this.vector.getField();
    }

    @Override
    public int getValueCapacity()
    {
        return this.vector.getValueCapacity();
    }

    protected int idx()
    {
        return super.idx();
    }

}
