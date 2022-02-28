package com.springbatchtest.config;

import com.springbatchtest.listener.JobCompletionListener;
import com.springbatchtest.partitioner.PorPartitioner;
import com.springbatchtest.step.Processor;
import com.springbatchtest.step.Reader;
import com.springbatchtest.step.Writer;
import com.springbatchtest.support.SortedPartitionStepBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.inject.Named;
import java.util.Comparator;

@Configuration
public class BatchConfig {

    private final Reader reader;
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    BatchConfig(Reader reader) {
        this.reader = reader;
    }


    @Bean
    public Job porReservationJob(@Named("partitionPerBankTaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        return jobBuilderFactory
                .get("preProcessReAuthorizationJob")
                .listener(listener())
                .start(partitionPerBankStep(taskExecutor))
                .build();
    }

    @Bean
    @JobScope
    protected Step partitionPerBankStep(@Named("partitionPerBankTaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        return new SortedPartitionStepBuilder(getDefaultStepBuilder(stepBuilderFactory, "partitionPerBankStep"),
                Comparator.comparing(StepExecution::getStepName))
                .partitioner("singleBankProcessor", new PorPartitioner())
                .allowStartIfComplete(false)
                .step(FraudRevalidationStep())
                .taskExecutor(taskExecutor)
                .build();
    }

    private StepBuilder getDefaultStepBuilder(StepBuilderFactory stepBuilderFactory, String stepName) {
        return stepBuilderFactory
                .get(stepName)
                .listener(listener());
    }

/*    @Bean
    public Job processJob() {
        return jobBuilderFactory.get("ReAuthJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener())
                .flow(ReAuthStep()).end().build();
    }*/

    @Bean
    public Step FraudRevalidationStep() {
        return stepBuilderFactory.get("FraudRevalidationStep")
                .<String, String>chunk(2)
                .reader(reader)
                .processor(new Processor())
                .writer(new Writer())
                .faultTolerant()
                //.skipLimit(2)
                //.skip(IllegalArgumentException.class)
                .retry(IllegalArgumentException.class)
                .build();
    }

    @Bean
    public JobExecutionListener listener() {
        return new JobCompletionListener();
    }

    @Bean(destroyMethod = "shutdown")
    protected ThreadPoolTaskExecutor partitionPerBankTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(2);
        taskExecutor.setMaxPoolSize(10);
        taskExecutor.setThreadNamePrefix("POR-PROCESSOR-");
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }
}