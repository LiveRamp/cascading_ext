package com.liveramp.cascading_ext.combiner;

import java.io.Serializable;

public interface ExactAggregator<T> extends FinalAggregator<T>, PartialAggregator<T>, Serializable {
}
