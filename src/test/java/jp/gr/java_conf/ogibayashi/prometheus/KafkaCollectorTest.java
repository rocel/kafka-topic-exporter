package jp.gr.java_conf.ogibayashi.prometheus;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import junit.framework.TestCase;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class KafkaCollectorTest extends TestCase {

    private ObjectMapper mapper = new ObjectMapper();
    private PropertyConfig counterConfig;
    private PropertyConfig gaugeConfig;

    public void setUp() {
        counterConfig = new PropertyConfig();
        counterConfig.set(PropertyConfig.Constants.EXPORTER_METRIC_TYPE.key, "counter");

        gaugeConfig = new PropertyConfig();
        gaugeConfig.set(PropertyConfig.Constants.EXPORTER_METRIC_TYPE.key, "gauge");

    }

    public void testAddSimpleMetric() {
        KafkaCollector collector = new KafkaCollector(counterConfig);
        final String logRecord = "{\"name\":\"foo\", \"value\": 9}";
        final String topic = "test.hoge";

        collector.add(topic, logRecord);
        List<MetricFamilySamples> mfsList = collector.collect();
        MetricFamilySamples mfs = mfsList.get(0);

        assertEquals("test_hoge_foo", mfs.name);
        assertEquals(Collector.Type.COUNTER, mfs.type);
        assertEquals("", mfs.help);
        assertEquals(9.0, mfs.samples.get(0).value);
    }

    public void testAddMetricWithLabel() throws IOException {
        KafkaCollector collector = new KafkaCollector(counterConfig);
        final String logRecord = "{\"name\":\"foo\", \"labels\": { \"label1\": \"v1\", \"lable2\": \"v2\" }, \"value\": 9}";
        final String topic = "test.hoge";
        KafkaExporterLogEntry jsonRecord = mapper.readValue(logRecord, KafkaExporterLogEntry.class);

        collector.add(topic, logRecord);
        List<MetricFamilySamples> mfsList = collector.collect();
        MetricFamilySamples mfs = mfsList.get(0);
        Map<String, String> labelMap = MetricUtil.getLabelMapFromSample(mfs.samples.get(0));

        assertEquals("test_hoge_foo", mfs.name);
        assertEquals(Collector.Type.COUNTER, mfs.type);
        assertEquals("", mfs.help);
        assertEquals(jsonRecord.getLabels(), labelMap);
        assertEquals(9.0, mfs.samples.get(0).value);
    }

    public void testAddMetricWithLabelAndTimestamp() throws IOException {
        KafkaCollector collector = new KafkaCollector(gaugeConfig);
        final String logRecord = "{\"name\":\"test.foo\", \"labels\": { \"label1\": \"v1\", \"lable2\": \"v2\" }, \"value\": 9, \"timestamp\": 1517330227}";
        final String topic = "test.hoge";
        KafkaExporterLogEntry jsonRecord = mapper.readValue(logRecord, KafkaExporterLogEntry.class);

        collector.add(topic, logRecord);
        List<MetricFamilySamples> mfsList = collector.collect();
        MetricFamilySamples mfs = mfsList.get(0);
        Map<String, String> labelMap = MetricUtil.getLabelMapFromSample(mfs.samples.get(0));

        assertEquals("test_hoge_test_foo", mfs.name);
        assertEquals(Collector.Type.GAUGE, mfs.type);
        assertEquals("", mfs.help);
        assertEquals(jsonRecord.getLabels(), labelMap);
        assertEquals(9.0, mfs.samples.get(0).value);
        assertEquals(1517330227, mfs.samples.get(0).timestampMs.longValue());
    }

    public void testIncrementValueWithSameLabel() throws IOException {
        KafkaCollector collector = new KafkaCollector(counterConfig);

        final String logRecord1 = "{\"name\":\"foo\", \"labels\": { \"label1\": \"v1\", \"label2\": \"v2\" }, \"value\": 9}";
        final String logRecord2 = "{\"name\":\"foo\", \"labels\": { \"label1\": \"aa1\", \"label2\": \"bb2\" }, \"value\": 10}";
        final String logRecord3 = "{\"name\":\"foo\", \"labels\": { \"label1\": \"v1\", \"label2\": \"v2\" }, \"value\": 18}";

        final String topic = "test.hoge";
        KafkaExporterLogEntry jsonRecord = mapper.readValue(logRecord3, KafkaExporterLogEntry.class);

        collector.add(topic, logRecord1);
        collector.add(topic, logRecord2);
        collector.add(topic, logRecord3);
        List<MetricFamilySamples> mfsList = collector.collect();
        MetricFamilySamples mfs = mfsList.get(0);
        List<MetricFamilySamples.Sample> samples = mfs.samples;

        assertEquals(2, samples.size());
        assertEquals(jsonRecord.getLabels(), MetricUtil.getLabelMapFromSample(samples.get(1)));
        assertEquals(27.0, samples.get(1).value);

    }

    public void testMetricExpire() throws IOException {
        PropertyConfig config = new PropertyConfig();
        config.set("exporter.metric.expire.seconds", "120");

        KafkaCollector collector = new KafkaCollector(config);
        LocalDateTime setDate1 = LocalDateTime.of(2016, 9, 20, 10, 0);
        LocalDateTime setDate2 = LocalDateTime.of(2016, 9, 20, 10, 9);
        LocalDateTime getDate = LocalDateTime.of(2016, 9, 20, 10, 10);

        final String logRecord1 = "{\"name\":\"foo\", \"labels\": { \"label1\": \"v1\", \"lable2\": \"v2\" }, \"value\": 9}";
        final String logRecord2 = "{\"name\":\"foo\", \"labels\": { \"label1\": \"aa1\", \"lable2\": \"bb2\" }, \"value\": 10}";
        final String topic = "test.hoge";
        KafkaExporterLogEntry jsonRecord = mapper.readValue(logRecord2, KafkaExporterLogEntry.class);

        collector.add(topic, logRecord1, setDate1);
        collector.add(topic, logRecord2, setDate2);

        List<MetricFamilySamples> mfsList = collector.collect(getDate);
        MetricFamilySamples mfs = mfsList.get(0);
        List<MetricFamilySamples.Sample> samples = mfs.samples;

        assertEquals(1, samples.size());
        assertEquals(jsonRecord.getLabels(), MetricUtil.getLabelMapFromSample(samples.get(0)));
        assertEquals(10.0, samples.get(0).value);
    }

}
