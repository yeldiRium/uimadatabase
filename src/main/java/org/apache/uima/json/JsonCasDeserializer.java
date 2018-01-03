/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.json;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.MorphologicalFeatures;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.NN;
import org.apache.commons.io.FileUtils;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.hucompute.services.type.DocElement;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;

/**
 * <h2>CAS serializer for JSON formats.</h2>
 * <p>Writes a CAS in a JSON format.</p>
 * <p>
 * <p>To use,</p>
 * <ul>
 * <li>create an instance of this class,</li>
 * <li>(optionally) configure the instance, and then</li>
 * <li>call serialize on the instance, optionally passing in additional parameters.</li></ul>
 * <p>
 * <p>After the 1st 2 steps, the serializer instance may be used for multiple calls (on multiple threads) to
 * the 3rd serialize step, if all calls use the same configuration.</p>
 * <p>
 * <p>There are "convenience" static serialize methods that do these three steps for common configurations.</p>
 * <p>
 * <p>Parameters can be configured in this instance (I), and/or as part of the serialize(S) call.</p>
 * <p>
 * <p>The parameters that can be configured are:</p>
 * <ul>
 * <li>(S) The CAS to serialize
 * <li>(S) where to put the output - an OutputStream, Writer, or File</li>
 * <li>(I,S) a type system - (default null) if supplied, it is used to "filter" types and features that are serialized.  If provided, only
 * those that exist in the passed in type system are included in the serialization</li>
 * <li>(I,S) a flag for prettyprinting - default false (no prettyprinting)</li>
 * </ul>
 * <p>
 * <p>For Json serialization, additional configuration from the Jackson implementation can be configured</p>
 * on 2 associated Jackson instances:
 * <ul><li>JsonFactory</li>
 * <li>JsonGenerator</li></ul>
 * using the standard Jackson methods on the associated JsonFactory instance;
 * see the Jackson JsonFactory and JsonGenerator javadocs for details.
 * <p>
 * <p>These 2 Jackson objects are settable/gettable from an instance of this class.
 * They are created if not supplied by the caller.</p>
 * <p>
 * <p>Once this instance is configured, the serialize method is called
 * to serialized a CAS to an output.</p>
 * <p>
 * <p>Instances of this class must be used on only one thread while configuration is being done;
 * afterwards, multiple threads may use the configured instance, to call serialize.</p>
 */
public class JsonCasDeserializer
{
	JSONObject context;

	public JsonCasDeserializer()
	{
		try
		{
			context = new JSONObject(FileUtils.readFileToString(new File("/home/ahemati/workspace/services/services-io/src/main/resources/context")));
		} catch (JSONException | IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public void deserialize(JCas cas, JSONObject json)
	{

		DocElement element = new DocElement(cas);
		element.setName("xyz");
		element.setBegin(0);
		element.setEnd(5000);
		element.addToIndexes();

		JSONObject initialView = json.getJSONObject("_views").getJSONObject("_InitialView");
		for (String key : initialView.keySet())
		{
			for (Object object : initialView.getJSONArray(key))
			{
				if (context.getJSONObject("_context").getJSONObject("_types").has(key))
				{
					createAnnotation(cas, (JSONObject) object, context.getJSONObject("_context").getJSONObject("_types").getJSONObject(key).getString("_id")).addToIndexes();
				}
			}
		}
	}

	private Annotation createAnnotation(JCas cas, JSONObject object, String name)
	{
		switch (name)
		{
			case "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.NN":
				return new NN(cas, object.getInt("b"), object.getInt("e"));
			case "de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.morph.MorphologicalFeatures":
				MorphologicalFeatures morph = new MorphologicalFeatures(cas, object.getInt("b"), object.getInt("e"));
				morph.setValue(object.getString("value"));
				return morph;
			default:
				return new Annotation(cas);
		}
	}

//	private JSONObject getReformatJson(JSONObject object){
//		object.put("begin", object.remove("b"));
//		object.put("end", object.remove("e"));
//		object.put("xd", object.remove("xmi:id"));
//		return object;
//	}

}
