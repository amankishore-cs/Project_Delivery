package com.cs.assets.services;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import com.cs.api.*;

@Path("/import")
public class AssetsService {
	@GET
	@Produces("application/json")
	public String getAssets() throws Exception {
		String sResponse = new SwiftAccountFilteredClone().execute();
		return sResponse;
	}
}
