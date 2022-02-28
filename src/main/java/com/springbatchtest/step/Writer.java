package com.springbatchtest.step;

import java.util.List;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;

public class Writer implements ItemWriter<String> {

	List<String> transactionIds;

	@Override
	public void write(List<? extends String> messages) throws Exception {
		//transactionIds = new ArrayList<>();
		System.out.println("-------> Writing the data " +  messages);
		//transactionIds.addAll(messages);
		/*for (String msg : messages) {
			if(msg.equalsIgnoreCase("transaction2")){
				System.out.println("throwing an error");
			throw new IllegalArgumentException("Arun throwing an exception at writer level.");}
		}*/
	}
}