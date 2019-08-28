// Copyright 2019, California Institute of Technology ("Caltech").
// U.S. Government sponsorship acknowledged.
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
// * Redistributions must reproduce the above copyright notice, this list of
// conditions and the following disclaimer in the documentation and/or other
// materials provided with the distribution.
// * Neither the name of Caltech nor its operating division, the Jet Propulsion
// Laboratory, nor the names of its contributors may be used to endorse or
// promote products derived from this software without specific prior written
// permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

package gov.nasa.pds.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import gov.nasa.pds.search.util.XssUtils;

/**
 * This class handles query requests conforming to IPDA's Planetary Data Access
 * Protocol (PDAP) and maps that query into a Solr conformant query for the
 * Search Service to process.
 *
 * @author pramirez
 */
public class PDAPHandler extends SearchHandler {

	public enum RESOURCE_CLASS {
		DATA_SET, PRODUCT, MAP_PROJECTED, METADATA;
	}

	private Logger LOG = Logger.getLogger(this.getClass().getName());
	private final static Map<RESOURCE_CLASS, Map<String, String>> resourceParams;
	private final static Map<String, String> resourceMap;
	private final static Map<String, String> generalParams;
	private final static List<String> rangedParams = new ArrayList<String>(Arrays.asList("INSTRUMENT_NAME",
			"INSTRUMENT_TYPE", "LATITUDE", "LONGITUDE", "START_TIME", "STOP_TIME", "TARGET_NAME", "TARGET_TYPE"));
	private final static String RESOURCE = "RESOURCE_CLASS";
	private final static String RETURN_TYPE = "RETURN_TYPE";
	private final static String PAGE_SIZE = "PAGE_SIZE";
	private final static String PAGE_NUMBER = "PAGE_NUMBER";
	private final static String VOTABLE = "VOTABLE";
	private final static String RESOURCE_FIELD = "objectType";

	static {
		resourceMap = new HashMap<String, String>();
		resourceMap.put("DATA_SET", "Product_Data_Set_PDS3");
		resourceMap.put("PRODUCT", "Product_Observational");
		resourceMap.put("MAP_PROJECTED", "Product_Observational");
		resourceMap.put("METADATA", "Product_Null");

		generalParams = new HashMap<String, String>();
		generalParams.put("INSTRUMENT_NAME", "instrument_name");
		generalParams.put("INSTRUMENT_TYPE", "instrument_type");
		generalParams.put("START_TIME", "start_time");
		generalParams.put("STOP_TIME", "stop_time");
		generalParams.put("TARGET_NAME", "target_name");
		generalParams.put("TARGET_TYPE", "target_type");
		Collections.unmodifiableMap(generalParams);

		resourceParams = new HashMap<RESOURCE_CLASS, Map<String, String>>();

		Map<String, String> datasetParams = new HashMap<String, String>();
		datasetParams.put("DATA_SET_ID", "data_set_id");
		Collections.unmodifiableMap(datasetParams);
		resourceParams.put(RESOURCE_CLASS.DATA_SET, datasetParams);

		Map<String, String> productParams = new HashMap<String, String>();
		productParams.put("DATA_SET_ID", "data_set_id");
		productParams.put("PRODUCT_ID", "product_id");
		Collections.unmodifiableMap(productParams);
		resourceParams.put(RESOURCE_CLASS.PRODUCT, productParams);

		Map<String, String> projectedParams = new HashMap<String, String>();
		projectedParams.put("LONGITUDE", "longitude");
		projectedParams.put("LATITUDE", "latitude");
		Collections.unmodifiableMap(projectedParams);
		resourceParams.put(RESOURCE_CLASS.MAP_PROJECTED, projectedParams);

		Map<String, String> metadataParams = new HashMap<String, String>();
		Collections.unmodifiableMap(metadataParams);
		resourceParams.put(RESOURCE_CLASS.METADATA, metadataParams);
	}

	private static void appendRanged(StringBuilder query, String parameter, String value) {
		String[] ranges = value.split(",");
		for (String range : ranges) {
			query.append(parameter);
			query.append(":");
			if (!range.contains("/")) {
				query.append(range);
			} else {
				String[] extrema = range.split("/");
				query.append("[");
				if (extrema.length == 1) {
					query.append(extrema[0]);
					query.append(" TO *");
				} else {
					if (extrema[0].length() == 0) {
						query.append("*");
					} else {
						query.append(extrema[0]);
					}
					query.append(" TO ");
					query.append(extrema[1]);
				}
				query.append("]");
			}
			query.append(" OR ");
		}
		// Remove the last OR
		query.delete(query.length() - 4, query.length());
	}

	@Override
	public void handleRequestBody(SolrQueryRequest request, SolrQueryResponse response) throws Exception {
		ModifiableSolrParams pdapParams = new ModifiableSolrParams(request.getParams());
		request.setParams(pdapParams);
		StringBuilder queryString = new StringBuilder();

		// Handle the general parameters that are the same across all resource
		// classes
		for (String parameter : generalParams.keySet()) {
			if (request.getOriginalParams().getParams(parameter) != null) {
				queryString.append("(");
				// Loop through and add in the identifier to the query string
				for (String value : request.getOriginalParams().getParams(parameter)) {
					if (!value.trim().isEmpty()) {
						if (rangedParams.contains(parameter)) {
							appendRanged(queryString, generalParams.get(parameter), XssUtils.clean(value));
						} else {
							queryString.append(generalParams.get(parameter));
							queryString.append(":");
							queryString.append(XssUtils.clean(value));
						}
						queryString.append(" OR ");
					}
				}
				// Remove the last OR
				queryString.delete(queryString.length() - 4, queryString.length());
				queryString.append(")");
				queryString.append(" AND ");
			}
		}

		// Remove the dangling AND
		if (queryString.length() != 0) {
			queryString.delete(queryString.length() - 5, queryString.length());
		}

		// Grab the resource class, default to METADATA if not specified.
		String resourceClass = null;
		RESOURCE_CLASS resource = null;
		if (request.getOriginalParams().getParams(RESOURCE) != null) {
			resourceClass = XssUtils.clean(request.getOriginalParams().getParams(RESOURCE)[0]);
			try {
				resource = RESOURCE_CLASS.valueOf(resourceClass);
			} catch (Exception e) {
				resourceClass = "METADATA";
				resource = RESOURCE_CLASS.valueOf(resourceClass);
				pdapParams.remove(RESOURCE);
				pdapParams.add(RESOURCE, "METADATA");
			}
		} else {
			resourceClass = "METADATA";
			resource = RESOURCE_CLASS.valueOf(resourceClass);
			pdapParams.add(RESOURCE, "METADATA");
		}
		Map<String, String> resourceParam = resourceParams.get(resource);

		// If there is already a portion of the query string group with AND
		if (queryString.length() != 0) {
			queryString.append(" AND ");
		}
		for (String parameter : resourceParam.keySet()) {
			if (request.getOriginalParams().getParams(parameter) != null) {
				queryString.append("(");
				// Loop through and add in the identifier to the query string
				for (String value : request.getOriginalParams().getParams(parameter)) {
					if (!value.trim().isEmpty()) {
						if (rangedParams.contains(parameter)) {
							appendRanged(queryString, resourceParam.get(parameter), XssUtils.clean(value));
						} else {
							queryString.append(resourceParam.get(parameter));
							queryString.append(":");
							queryString.append(XssUtils.clean(value));
						}
						queryString.append(" OR ");
					}
				}
				// Remove the last OR
				queryString.delete(queryString.length() - 4, queryString.length());
				queryString.append(")");
				queryString.append(" AND ");
			}
		}

		// Inject the resource class into the query
		queryString.append(RESOURCE_FIELD);
		queryString.append(":");
		queryString.append(resourceMap.get(resourceClass));

		// If there is a query pass it on to Solr as the q param
		if (queryString.length() > 0) {
			pdapParams.remove("q");
			pdapParams.add("q", queryString.toString());
		}

		// Handle return type maps to Solrs wt param
		if (request.getOriginalParams().getParams(RETURN_TYPE) != null) {
			String returnType = XssUtils.clean(request.getOriginalParams().getParams(RETURN_TYPE)[0]);
			if (!VOTABLE.equals(returnType)) {
				// Just use Solr's default response writers
				pdapParams.remove("wt");
				pdapParams.add("wt", XssUtils.clean(request.getOriginalParams().getParams(RETURN_TYPE)[0]));
			}
		}

		// Handle the page size if specified. If not, get the default page size.
		int pageSize = 0;
		if (request.getOriginalParams().getParams(PAGE_SIZE) != null) {
			pageSize = Integer.parseInt(XssUtils.clean(request.getOriginalParams().getParams(PAGE_SIZE)[0]));
			pdapParams.remove("rows");
			pdapParams.add("rows", "" + pageSize);
		} else {
			pageSize = Integer.parseInt(XssUtils.clean(request.getParams().getParams("rows")[0]));
		}

		// Handle the page number and convert it for Solr's start parameter.
		if (request.getOriginalParams().getParams(PAGE_NUMBER) != null) {
			int pageNum = Integer.parseInt(XssUtils.clean(request.getOriginalParams().getParams(PAGE_NUMBER)[0]));
			if (pageNum > 1) {
				pdapParams.remove("start");
				pdapParams.add("start", "" + (pageNum * pageSize - pageSize));
			}
		}

		this.LOG.info("Solr Query String: " + queryString);
		super.handleRequestBody(request, response);
	}

}
