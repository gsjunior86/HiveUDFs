package br.gsj.bigdata.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * GenericUDTFExplode.
 *
 */
@Description(name = "explode", value = "_FUNC_(a) - separates the elements of array a into multiple rows,"
		+ " or the elements of a map into multiple rows and columns ")
public class ExplodeHotelResults extends GenericUDTF {

	private transient ObjectInspector inputOI = null;

	@Override
	public void close() throws HiveException {
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if (args.length != 1) {
			throw new UDFArgumentException("explode() takes only one argument");
		}

		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		switch (args[0].getCategory()) {
		case LIST:
			inputOI = args[0];
			fieldNames.add("col");
			fieldOIs.add(((ListObjectInspector) inputOI).getListElementObjectInspector());
			break;
		case MAP:
			inputOI = args[0];
			fieldNames.add("hotel_id");
			fieldNames.add("advertisers");
			fieldOIs.add(((MapObjectInspector) inputOI).getMapKeyObjectInspector());
			fieldOIs.add(((MapObjectInspector) inputOI).getMapValueObjectInspector());
			break;
		default:
			throw new UDFArgumentException("explode() takes an array or a map as a parameter");
		}

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	private transient final Object[] forwardListObj = new Object[1];
	private transient final Object[] forwardMapObj = new Object[2];

	@Override
	public void process(Object[] o) throws HiveException {
		switch (inputOI.getCategory()) {
		case LIST:
			ListObjectInspector listOI = (ListObjectInspector) inputOI;
			List<?> list = listOI.getList(o[0]);
			if (list == null) {
				return;
			}
			for (Object r : list) {
				forwardListObj[0] = r;
				forward(forwardListObj);
			}
			break;
		case MAP:
			MapObjectInspector mapOI = (MapObjectInspector) inputOI;
			Map<?, ?> map = mapOI.getMap(o[0]);
			if (map == null) {
				return;
			}
			for (Entry<?, ?> r : map.entrySet()) {
				forwardMapObj[0] = r.getKey();
				forwardMapObj[1] = r.getValue();
				forward(forwardMapObj);
			}
			break;
		default:
			throw new TaskExecutionException("explode() can only operate on an array or a map");
		}
	}

	@Override
	public String toString() {
		return "explode";
	}
}
