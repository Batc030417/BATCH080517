package com.laboros;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class DeIdentifyUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException 
	{
		
		return null;
	}

}
