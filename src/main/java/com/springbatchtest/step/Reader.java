package com.springbatchtest.step;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
@StepScope
public class Reader implements StepExecutionListener, ItemReader<String> {

    private final List<String> messages1 = new ArrayList<>();
    private final List<String> messages2 = new ArrayList<>();
    private final List<String> messages3 = new ArrayList<>();
    private final List<String> messages4 = new ArrayList<>();
    private final String thread;

    public Reader(@Value("#{stepExecutionContext['name']}") String thread) {
        this.thread = thread;
    }

    @Override
    public String read() throws Exception {
        String result;

        if (Objects.equals(thread, "Thread 4201") && messages1.size() > 0) {
            result = messages1.get(0);
            messages1.remove(result);
            System.out.println(thread + " reading : " + result);
            return result;
        }

        if (Objects.equals(thread, "Thread 1802") && messages2.size() > 0) {
            result = messages2.get(0);
            messages2.remove(result);
            System.out.println(thread + " reading : " + result);
            return result;
        }


        if (Objects.equals(thread, "Thread 2544") && messages3.size() > 0) {
            result = messages3.get(0);
            messages3.remove(result);
            System.out.println(thread + " reading : " + result);
            return result;
        }

        if (Objects.equals(thread, "Thread 35083100") && messages4.size() > 0) {
            result = messages4.get(0);
            messages4.remove(result);
            System.out.println(thread + " reading : " + result);
            return result;
        }

        return null;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        messages1.add("Trans_4201_1");
        messages1.add("Trans_4201_2");
        messages1.add("Trans_4201_3");

        messages2.add("Trans_1802_1");
        messages2.add("Trans_1802_2");
        messages2.add("Trans_1802_3");

        messages3.add("Trans_2544_1");
        messages3.add("Trans_2544_2");
        messages3.add("Trans_2544_3");

        messages4.add("Trans_CAVA_1");
        messages4.add("Trans_CAVA_2");
        messages4.add("Trans_CAVA_3");

    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }
}