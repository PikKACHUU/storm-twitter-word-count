package com.kaviddiss.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Keeps stats on word count, calculates and logs top words every X second to stdout and top list every Y seconds,
 *
 * @author davidk
 */
public class WordCounterBolt extends BaseRichBolt {

    private static final long serialVersionUID = 2706047697068872387L;

    private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);

    /**
     * Number of seconds before the top list will be logged to stdout.
     */
    private final long logIntervalSec;

    /**
     * Number of top words to store in stats.
     */
    private final int topListSize;

    private Map<String, Integer> counter;
    private long lastLogTime;

    public WordCounterBolt(long logIntervalSec, int topListSize) {
        this.logIntervalSec = logIntervalSec;
        this.topListSize = topListSize;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        counter = new HashMap<String, Integer>();
        lastLogTime = System.currentTimeMillis();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        Integer count = counter.get(word);
        if(count == null)
            count = 0;
        count++;
        counter.put(word, count);

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {
            logger.info("\n\n");
            logger.info("Word count: " + counter.size());
            publishTopList();
            lastLogTime = now;
        }
    }

    private void publishTopList() {
        // calculate top list:
        SortedMap<Integer, String> top = new TreeMap<Integer, String>();
        for (Map.Entry<String,Integer> entry : counter.entrySet()) {
            int count = entry.getValue();
            String word = entry.getKey();

            top.put(count, word);
            if (top.size() > topListSize) {
                top.remove(top.firstKey());
            }
        }

        for (Map.Entry<Integer, String> entry : top.entrySet()) {

        // Output top list:
            logger.info(new StringBuilder("top - ").append(entry.getValue()).append('|').append(entry.getKey()).toString());
        }

    }
}
