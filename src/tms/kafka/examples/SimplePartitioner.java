package tms.kafka.examples;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
 
public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {
 
    }
 
    public int partition(Object arg0, int a_numPartitions) {
    	String key = (String)arg0;
        int partition = 0;
        int offset = key.lastIndexOf('.');
        if (offset > 0) {
           partition = Integer.parseInt( key.substring(offset+1)) % a_numPartitions;
        }
       return partition;
  }

}