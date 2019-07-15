package br.gsj.bigdata.hive;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ExplodeHotelResults extends GenericUDTF{
	
	  private transient ObjectInspector inputOI = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 1) {
		      throw new UDFArgumentException("This function requires the hotel_results map");
		    }
		
	    ArrayList<String> fieldNames = new ArrayList<String>();
	    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		
		switch (args[0].getCategory()) {
		case MAP:
		      inputOI = args[0];
		      fieldNames.add("hotel_id");
		      fieldNames.add("advertisers");
		      fieldOIs.add(((MapObjectInspector)inputOI).getMapKeyObjectInspector());
		      fieldOIs.add(((MapObjectInspector)inputOI).getMapValueObjectInspector());
		      break;
		default:
		      throw new UDFArgumentException("hotelRes() takes a map as a parameter");
		}
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
		        fieldOIs);
	}
	
	  private transient final Object[] forwardMapObj = new Object[2];

	  @Override
	  public void process(Object[] o) throws HiveException {
	    switch (inputOI.getCategory()) {
	    case MAP:
	      MapObjectInspector mapOI = (MapObjectInspector)inputOI;
	      Map<?,?> map = mapOI.getMap(o[0]);
	      if (map == null) {
	        return;
	      }
	      for (Entry<?,?> r : map.entrySet()) {
	          forwardMapObj[0] = r.getKey();
	          forwardMapObj[1] = r.getValue();
	          forward(forwardMapObj);
	        }
	      break;
	    default:
	      throw new TaskExecutionException("explodeHotelResults() can only operate on a map");
	    }
	  }

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
		
	}


	
	
	

}
