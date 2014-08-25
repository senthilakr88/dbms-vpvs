package edu.buffalo.cse562.structure;

import java.io.IOException;
import java.util.List;

import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class indexlonglistserializable implements Serializer<List<Long>> {

	@Override
	public List<Long> deserialize(SerializerInput in) throws IOException,
			ClassNotFoundException {
		return in.readObject();
	}

	@Override
	public void serialize(SerializerOutput out, List<Long> idList)
			throws IOException {
		out.writeObject(idList);
	}

}
