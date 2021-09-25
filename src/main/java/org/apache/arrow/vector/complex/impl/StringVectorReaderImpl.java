package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.StringVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class StringVectorReaderImpl extends AbstractFieldReader
{
    private final StringVector vector;

    public StringVectorReaderImpl(StringVector vector)
    {
        this.vector = vector;
    }

    @Override
    public Types.MinorType getMinorType()
    {
        return this.vector.getMinorType();
    }

    @Override
    public Field getField()
    {
        return this.vector.getField();
    }

    public boolean isSet()
    {
        return !this.vector.isNull(this.idx());
    }

    public Object readObject()
    {
        return this.vector.getObject(this.idx());
    }
}
