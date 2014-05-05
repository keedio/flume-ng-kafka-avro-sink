/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/

package org.redoop.flume.sink.avro.kafka.parsers;

import java.util.HashMap;

public class LinuxAuditParser implements Parser{
	
	public static final String ADD_GROUP = "ADD_GROUP";// TODO
	public static final String ADD_USER = "ADD_USER";// TODO
	
	public static final String CRED_ACQ = "CRED_ACQ"; // DONE
	public static final String CRED_DISP = "CRED_DISP"; // DONE
	public static final String CRED_REFR = "CRED_REFR"; //TODO
	
	public static final String CRYPTO_KEY_USER = "CRYPTO_KEY_USER"; //TODO
	public static final String CRYPTO_SESSION = "CRYPTO_SESSION"; // DONE
	
	public static final String DEL_GROUP = "DEL_GROUP";// TODO
	public static final String DEL_USER = "DEL_USER";// TODO
	
	public static final String LOGIN = "LOGIN"; // DONE
	
	public static final String USER_ACCT = "USER_ACCT"; // DONE
	public static final String USER_AUTH = "USER_AUTH"; // DONE
	public static final String USER_END = "USER_END"; // DONE
	public static final String USER_LOGIN = "USER_LOGIN"; // DONE
	public static final String USER_LOGOUT = "USER_LOGOUT"; // DONE
	public static final String USER_START = "USER_START"; // DONE

	@Override
	public HashMap<String, Object> init(String line) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		String fields[] = line.split(" ");
		String type = fields[1].split("=")[1];
		switch(type){
		
			case(ADD_GROUP):
				//TODO
				break;
			case(ADD_USER):
				//TODO
				break;
			case(CRED_ACQ):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				map.put("aux", fields[3]);
				map.put("pid", fields[4].split("=")[1]);
				map.put("uid", fields[5].split("=")[1]);
				map.put("auid", fields[6].split("=")[1]);
				map.put("ses", fields[7].split("=")[1]);
				map.put("op", fields[8].split("=")[2]);
				map.put("acct", fields[9].split("=")[1]);
				map.put("exe", fields[10].split("=")[1]);
				map.put("hostname", fields[11].split("=")[1]);
				map.put("addr", fields[12].split("=")[1]);
				map.put("terminal", fields[13].split("=")[1]);
				map.put("res", fields[14].split("=")[1]);
				break;
			case(CRED_DISP):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				map.put("aux", fields[3]);
				map.put("pid", fields[4].split("=")[1]);
				map.put("uid", fields[5].split("=")[1]);
				map.put("auid", fields[6].split("=")[1]);
				map.put("ses", fields[7].split("=")[1]);
				map.put("op", fields[8].split("=")[2]);
				map.put("acct", fields[9].split("=")[1]);
				map.put("exe", fields[10].split("=")[1]);
				map.put("hostname", fields[11].split("=")[1]);
				map.put("addr", fields[12].split("=")[1]);
				map.put("terminal", fields[13].split("=")[1]);
				map.put("res", fields[14].split("=")[1]);
				break;
			case (CRED_REFR):
				//TODO
				break;
			case(CRYPTO_KEY_USER):
				//TODO
				break;
			case(CRYPTO_SESSION):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				// AUX FIELD CONTAINS:
				//		USER, DIRECTION, CIPHER, KSIZE, SPID
				// 		SUID, RPORT, LADDR, LPORT
				// IN FORMAT (example): "user direction=from-client cipher=aes128-ctr ksize=128 spid=29863 suid=74 rport=57159 laddr=192.10.0.63 lport=22"				
				map.put("aux",fields[3] + " "
							+ fields[9] + " "
							+ fields[10] + " "
							+ fields[11] + " "
							+ fields[12] + " "
							+ fields[13] + " "
							+ fields[14] + " "
							+ fields[15] + " "
							+ fields[16] + " ");
				map.put("pid", fields[4].split("=")[1]);
				map.put("uid", fields[5].split("=")[1]);
				map.put("auid", fields[6].split("=")[1]);
				map.put("ses", fields[7].split("=")[1]);
				map.put("op", fields[8].split("=")[2]);
				map.put("acct", "");
				map.put("exe", fields[18].split("=")[1]);
				map.put("hostname", fields[19].split("=")[1]);
				map.put("addr", fields[20].split("=")[1]);
				map.put("terminal", fields[21].split("=")[1]);
				map.put("res", fields[22].split("=")[1]);
				break;
			case(DEL_GROUP):
				//TODO
				break;
			case(DEL_USER):
				//TODO
				break;
			case(LOGIN):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				// AUX FIELD CONTAINS OLD AUID AND OLD SES FIELDS
				// IN FORMAT: "old auid=4294967295 old ses=4294967295"
				map.put("aux", fields[5] + " " + fields[6] + " " + fields[9] + " " + fields[10]);
				map.put("pid", fields[3].split("=")[1]);
				map.put("uid", fields[4].split("=")[1]);
				map.put("auid", fields[8].split("=")[1]);
				map.put("ses", fields[12].split("=")[1]);
				map.put("op", "");
				map.put("acct", "");
				map.put("exe", "");
				map.put("hostname", "");
				map.put("addr", "");
				map.put("terminal", "");
				map.put("res", "");
				break;
			case(USER_ACCT):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				map.put("aux", fields[3]);
				map.put("pid", fields[4].split("=")[1]);
				map.put("uid", fields[5].split("=")[1]);
				map.put("auid", fields[6].split("=")[1]);
				map.put("ses", fields[7].split("=")[1]);
				map.put("op", fields[8].split("=")[2]);
				map.put("acct", fields[9].split("=")[1]);
				map.put("exe", fields[10].split("=")[1]);
				map.put("hostname", fields[11].split("=")[1]);
				map.put("addr", fields[12].split("=")[1]);
				map.put("terminal", fields[13].split("=")[1]);
				map.put("res", fields[14].split("=")[1]);
				break;
			case(USER_AUTH):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				map.put("aux", fields[3]);
				map.put("pid", fields[4].split("=")[1]);
				map.put("uid", fields[5].split("=")[1]);
				map.put("auid", fields[6].split("=")[1]);
				map.put("ses", fields[7].split("=")[1]);
				map.put("op", fields[8].split("=")[2]);
				map.put("acct", fields[9].split("=")[1]);
				map.put("exe", fields[10].split("=")[1]);
				map.put("hostname", fields[11].split("=")[1]);
				map.put("addr", fields[12].split("=")[1]);
				map.put("terminal", fields[13].split("=")[1]);
				map.put("res", fields[14].split("=")[1]);
				break;
			case(USER_END):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				map.put("aux", fields[3]);
				map.put("pid", fields[4].split("=")[1]);
				map.put("uid", fields[5].split("=")[1]);
				map.put("auid", fields[6].split("=")[1]);
				map.put("ses", fields[7].split("=")[1]);
				map.put("op", fields[8].split("=")[2]);
				map.put("acct", fields[9].split("=")[1]);
				map.put("exe", fields[10].split("=")[1]);
				map.put("hostname", fields[11].split("=")[1]);
				map.put("addr", fields[12].split("=")[1]);
				map.put("terminal", fields[13].split("=")[1]);
				map.put("res", fields[14].split("=")[1]);
				break;
			case(USER_LOGIN):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				map.put("aux", fields[3]);
				map.put("pid", fields[4].split("=")[1]);
				map.put("uid", fields[5].split("=")[1]);
				map.put("auid", fields[6].split("=")[1]);
				map.put("ses", fields[7].split("=")[1]);
				map.put("op", fields[8].split("=")[2]);
				map.put("acct", fields[9].split("=")[1]);
				map.put("exe", fields[10].split("=")[1]);
				map.put("hostname", fields[11].split("=")[1]);
				map.put("addr", fields[12].split("=")[1]);
				map.put("terminal", fields[13].split("=")[1]);
				map.put("res", fields[14].split("=")[1]);
				break;
			case(USER_LOGOUT):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				map.put("aux", fields[3]);
				map.put("pid", fields[4].split("=")[1]);
				map.put("uid", fields[5].split("=")[1]);
				map.put("auid", fields[6].split("=")[1]);
				map.put("ses", fields[7].split("=")[1]);
				map.put("op", fields[8].split("=")[2]);
				map.put("acct", fields[9].split("=")[1]);
				map.put("exe", fields[10].split("=")[1]);
				map.put("hostname", fields[11].split("=")[1]);
				map.put("addr", fields[12].split("=")[1]);
				map.put("terminal", fields[13].split("=")[1]);
				map.put("res", fields[14].split("=")[1]);
				break;
			case(USER_START):
				map.put("node", fields[0].split("=")[1]);
				map.put("type", type);
				map.put("msgaudit", fields[2].split("=")[1]);
				map.put("aux", fields[3]);
				map.put("pid", fields[4].split("=")[1]);
				map.put("uid", fields[5].split("=")[1]);
				map.put("auid", fields[6].split("=")[1]);
				map.put("ses", fields[7].split("=")[1]);
				map.put("op", fields[8].split("=")[2]);
				map.put("acct", fields[9].split("=")[1]);
				map.put("exe", fields[10].split("=")[1]);
				map.put("hostname", fields[11].split("=")[1]);
				map.put("addr", fields[12].split("=")[1]);
				map.put("terminal", fields[13].split("=")[1]);
				map.put("res", fields[14].split("=")[1]);
				break;		
		
		}
		
		return map;
	}

}
