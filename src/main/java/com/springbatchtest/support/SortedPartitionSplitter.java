package com.springbatchtest.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.StepExecutionSplitter;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Sorts the partitions. Default is by the natural sorting of the key from {@link org.springframework.batch.core.partition.support.Partitioner#partition(int)}
 *
 * @see SortedPartitionStepBuilder
 */
public class SortedPartitionSplitter implements StepExecutionSplitter {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final StepExecutionSplitter delegate;
    private final Comparator<StepExecution> comparator;

    public SortedPartitionSplitter(StepExecutionSplitter delegate, Comparator<StepExecution> comparator) {
        this.delegate = delegate;
        this.comparator = comparator;
    }

    @Override
    public String getStepName() {
        return delegate.getStepName();
    }

    @Override
    public Set<StepExecution> split(StepExecution stepExecution, int gridSize) throws JobExecutionException {
        Set<StepExecution> sorted = new TreeSet<>(comparator);
        delegate.split(stepExecution, gridSize).forEach(sorted::add);
        logger.debug("Sorted StepExecutions: {}", sorted);
        return sorted;
    }

}
