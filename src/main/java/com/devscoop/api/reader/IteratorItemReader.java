package com.devscoop.api.reader;

import org.springframework.batch.item.ItemReader;

import java.util.Iterator;

public class IteratorItemReader<T> implements ItemReader<T> {
    private final Iterator<T> iterator;

    public IteratorItemReader(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public T read() {
        return iterator.hasNext() ? iterator.next() : null;
    }
}
