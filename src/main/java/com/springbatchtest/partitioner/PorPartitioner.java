package com.springbatchtest.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to partition the POR batch based on the initiator bank Ids.
 *
 * @author e210849
 */
@Component
@StepScope
public class PorPartitioner implements Partitioner {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<String> initiatorBankIds;

    public PorPartitioner() {
        initiatorBankIds = List.of("4201", "1802","2544", "35083100");
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        logger.info("Partition will be performed on banks {} ({} total)", initiatorBankIds, initiatorBankIds.size());
        Map<String, ExecutionContext> result = new LinkedHashMap<>();
        int i = 1;
        for (String initiatorBankId : initiatorBankIds) {
            ExecutionContext value = new ExecutionContext();
            value.putString("initiatorBankId", initiatorBankId);
            value.putString("name", String.format("Thread %s", initiatorBankId));
            result.put(String.format("Thread %s", i++), value);
        }
        System.out.println(result);
        return result;
    }

}
