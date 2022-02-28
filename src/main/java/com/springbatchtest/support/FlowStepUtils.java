package com.springbatchtest.support;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowStep;

import java.util.Objects;

public class FlowStepUtils {

    private FlowStepUtils() {
    }

    /**
     * When we have a 'flow' step, there is no type of FlowScope in spring batch. JobScope and StepScope exist,
     * but it is not possible to promote a step execution context property to the flow's scope.
     * <p>
     * By using this, it is possible to find the Flow's StepExecution from an inner step execution by providing
     * some sort of unique identifier that is constant throughout the flow's scope.
     *
     * @param stepExecution
     * @param matchingKey
     * @return StepExecution, the flow scope step execution if found, otherwise the input is returned
     */
    public static StepExecution findFlowStepExecution(StepExecution stepExecution, String matchingKey) {
        for (StepExecution someStep : stepExecution.getJobExecution().getStepExecutions()) {
            if (FlowStep.class.getName().equals(someStep.getExecutionContext().get(Step.STEP_TYPE_KEY)) &&
                    Objects.equals(someStep.getExecutionContext().get(matchingKey),
                            stepExecution.getExecutionContext().get(matchingKey))) {
                return someStep;
            }
        }
        return stepExecution;
    }
}
