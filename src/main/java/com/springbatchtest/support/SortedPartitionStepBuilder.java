package com.springbatchtest.support;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.support.PartitionStep;
import org.springframework.batch.core.step.builder.PartitionStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilderHelper;

import java.util.Comparator;

/**
 * Sorts the partitions by the natural sorting of the key from {@link org.springframework.batch.core.partition.support.Partitioner#partition(int)}
 *
 * @see SortedPartitionSplitter
 */
public class SortedPartitionStepBuilder extends PartitionStepBuilder {

    private final Comparator<StepExecution> comparator;

    public SortedPartitionStepBuilder(StepBuilderHelper<?> parent,  Comparator<StepExecution> comparator) {
        super(parent);
        this.comparator = comparator;
    }
    @Override
    public Step build() {
        PartitionStep step = (PartitionStep) super.build();
        step.setStepExecutionSplitter(new SortedPartitionSplitter(this.getSplitter(), comparator));
        return step;
    }
}
