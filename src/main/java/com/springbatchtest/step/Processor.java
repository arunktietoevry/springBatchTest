package com.springbatchtest.step;

import org.springframework.batch.item.ItemProcessor;

public class Processor implements ItemProcessor<String, String> {

	@Override
	public String process(String data) throws Exception {
		//System.out.println("-----------> From processor : "+data.toUpperCase());
		System.out.println("processing : "+data.toUpperCase());
		return data.toUpperCase();
	}

}