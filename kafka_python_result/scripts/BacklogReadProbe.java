import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

/** 고정된 과거 offset 범위를 한 번 읽는다. producer run의 sequence oracle과 분리한다. */
public final class BacklogReadProbe {
    public static void main(String[] args) throws Exception {
        Path log=Path.of(args[2]), root=log.getParent();
        Properties p=new Properties();
        p.put("bootstrap.servers",args[0]); p.put("group.id","backlog-"+UUID.randomUUID());
        p.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("enable.auto.commit","false");p.put("auto.offset.reset","none");
        p.put("max.poll.records","10000");p.put("max.partition.fetch.bytes","1048576");
        try(KafkaConsumer<byte[],byte[]> c=new KafkaConsumer<>(p)) {
            List<TopicPartition> parts=new ArrayList<>();
            c.partitionsFor(args[1]).forEach(x->parts.add(new TopicPartition(args[1],x.partition())));
            if(parts.size()!=8)throw new IllegalStateException("8 partitions required");
            c.assign(parts);
            Map<TopicPartition,Long> start=c.beginningOffsets(parts), end=c.endOffsets(parts);
            Map<TopicPartition,Long> next=new HashMap<>(start);
            for(TopicPartition tp:parts)c.seek(tp,start.get(tp));
            Files.writeString(root.resolve("backlog-ready.json"),"{\"start\":"+offsets(start)+",\"end\":"+offsets(end)+"}");
            long readyDeadline=System.nanoTime()+Duration.ofSeconds(60).toNanos();
            while(!Files.exists(root.resolve("backlog-go"))) {
                if(System.nanoTime()>readyDeadline)throw new IllegalStateException("measurement gate timeout");
                Thread.sleep(50);
            }
            long begin=System.nanoTime(), deadline=begin+Duration.ofSeconds(Long.parseLong(args[3])).toNanos();
            long count=0,bytes=0,last=0;
            while(true) {
                boolean done=true;
                for(TopicPartition tp:parts)if(next.get(tp)<end.get(tp))done=false;
                if(done) {
                    Files.writeString(root.resolve("backlog-complete.json"),"{\"epoch_ms\":"+System.currentTimeMillis()+",\"records\":"+count+",\"payload_bytes\":"+bytes+",\"seconds\":"+((System.nanoTime()-begin)/1e9)+"}");
                    break;
                }
                if(System.nanoTime()>deadline)throw new IllegalStateException("fixed backlog unfinished at deadline");
                ConsumerRecords<byte[],byte[]> batch=c.poll(Duration.ofMillis(100));
                for(ConsumerRecord<byte[],byte[]> r:batch) {
                    TopicPartition tp=new TopicPartition(r.topic(),r.partition());
                    if(r.offset()>=end.get(tp))continue;
                    if(r.offset()!=next.get(tp))throw new IllegalStateException("offset gap/regression or retention loss: "+tp);
                    next.put(tp,r.offset()+1);count++;bytes+=r.value()==null?0:r.value().length;
                }
                for(TopicPartition tp:parts)if(next.get(tp)>=end.get(tp))c.pause(List.of(tp));
                long elapsed=(System.nanoTime()-begin)/1_000_000_000;
                if(elapsed!=last) {
                    last=elapsed;
                    Map<TopicPartition,Long> latest=c.endOffsets(parts);
                    long lag=0;for(TopicPartition tp:parts)lag+=Math.max(0,latest.get(tp)-next.get(tp));
                    Files.writeString(log,"{\"epoch_ms\":"+System.currentTimeMillis()+",\"seconds\":"+elapsed+",\"records\":"+count+",\"payload_bytes\":"+bytes+",\"lag_records\":"+lag+"}\n",StandardOpenOption.CREATE,StandardOpenOption.APPEND);
                }
            }
        } catch(Exception error) {
            Files.writeString(root.resolve("backlog-failed.txt"),error.toString());
            throw error;
        }
    }
    private static String offsets(Map<TopicPartition,Long> values) {
        StringJoiner j=new StringJoiner(",","{","}");
        values.forEach((tp,value)->j.add("\""+tp.partition()+"\":"+value));return j.toString();
    }
}
