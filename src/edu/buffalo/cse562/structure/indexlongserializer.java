package edu.buffalo.cse562.structure;

import java.io.IOException;

import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class indexlongserializer implements Serializer<Long> {

	@Override
	public Long deserialize(SerializerInput in) throws IOException,
			ClassNotFoundException {
		Long id = in.readLong();
		return id;
	}

	@Override
	public void serialize(SerializerOutput out, Long id) throws IOException {
		out.writeLong(id);		
	}

}
