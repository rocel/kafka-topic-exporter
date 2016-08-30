package jp.gr.java_conf.ogibayashi.prometheus;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.List;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.io.IOException;

public class KafkaCollectorTest extends TestCase
{
    private ObjectMapper mapper = new ObjectMapper();

    public void testAddSimpleMetric() {
        KafkaCollector collector = new KafkaCollector();
        final String logRecord = "{\"name\":\"foo\", \"value\": 9}";
        final String topic = "test.hoge";

        collector.add(topic, logRecord);
        List<MetricFamilySamples> mfsList = collector.collect();
        MetricFamilySamples mfs = mfsList.get(0);
        
        assertEquals("test_hoge_foo", mfs.name);
        assertEquals(Collector.Type.GAUGE, mfs.type);
        assertEquals("", mfs.help);
        assertEquals(9.0, mfs.samples.get(0).value);
    }
    
    public void testAddMetricWithLabel() throws IOException {
        KafkaCollector collector = new KafkaCollector();
        final String logRecord = "{\"name\":\"foo\", \"labels\": { \"label1\": \"v1\", \"lable2\": \"v2\" }, \"value\": 9}";
        final String topic = "test.hoge";
        KafkaExporterLogEntry jsonRecord = mapper.readValue(logRecord, KafkaExporterLogEntry.class);
        
        collector.add(topic, logRecord);
        List<MetricFamilySamples> mfsList = collector.collect();
        MetricFamilySamples mfs = mfsList.get(0);
        Map<String, String> labelMap = MetricUtil.getLabelMapFromSample(mfs.samples.get(0));
        
        assertEquals("test_hoge_foo", mfs.name);
        assertEquals(Collector.Type.GAUGE, mfs.type);
        assertEquals("", mfs.help);
        assertEquals(jsonRecord.getLabels(), labelMap);
        assertEquals(9.0, mfs.samples.get(0).value);
    }
    
    public void testReplaceValueWithSameLabel() throws IOException {
        KafkaCollector collector = new KafkaCollector();

        final String logRecord1 = "{\"name\":\"foo\", \"labels\": { \"label1\": \"v1\", \"lable2\": \"v2\" }, \"value\": 9}";
        final String logRecord2 = "{\"name\":\"foo\", \"labels\": { \"label1\": \"aa1\", \"lable2\": \"bb2\" }, \"value\": 10}";
        final String logRecord3 = "{\"name\":\"foo\", \"labels\": { \"label1\": \"v1\", \"lable2\": \"v2\" }, \"value\": 18}";

        final String topic = "test.hoge";
        KafkaExporterLogEntry jsonRecord = mapper.readValue(logRecord3, KafkaExporterLogEntry.class);
        
        collector.add(topic, logRecord1);
        collector.add(topic, logRecord2);
        collector.add(topic, logRecord3);
        List<MetricFamilySamples> mfsList = collector.collect();
        MetricFamilySamples mfs = mfsList.get(0);
        List<MetricFamilySamples.Sample> samples = mfs.samples;

        assertEquals(2, samples.size());
        assertEquals(jsonRecord.getLabels(), MetricUtil.getLabelMapFromSample(samples.get(0)));
        assertEquals(18.0, samples.get(0).value);
            
    }
    
}
