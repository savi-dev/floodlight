package net.floodlightcontroller.staticflowentry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import java.util.Map;

import net.floodlightcontroller.core.IFloodlightProviderService;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.annotations.LogMessageCategory;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;

import net.floodlightcontroller.restserver.IRestApiService;
import net.floodlightcontroller.staticflowentry.web.StaticFlowEntryWebRoutable;


import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.util.HexString;
import org.openflow.util.U16;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogMessageCategory("Janus Pusher")
/***
 * 
 * @author Ke Wang   email: klwangke@163.com
 * This module is responsible for receiving information from Janus to manipulate the switches.
 *
 */
public class JanusPusher implements IFloodlightModule {

	protected static Logger log = LoggerFactory.getLogger(JanusPusher.class);
    public static final String StaticFlowName = "staticflowentry";

    public static final String TABLE_NAME = "controller_staticflowtableentry";
    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_SWITCH = "switch_id";
    public static final String COLUMN_ACTIVE = "active";
    public static final String COLUMN_IDLE_TIMEOUT = "idle_timeout";
    public static final String COLUMN_HARD_TIMEOUT = "hard_timeout";
    public static final String COLUMN_PRIORITY = "priority";
    public static final String COLUMN_COOKIE = "cookie";
    public static final String COLUMN_WILDCARD = "wildcards";
    public static final String COLUMN_IN_PORT = "in_port";
    public static final String COLUMN_DL_SRC = "dl_src";
    public static final String COLUMN_DL_DST = "dl_dst";
    public static final String COLUMN_DL_VLAN = "dl_vlan";
    public static final String COLUMN_DL_VLAN_PCP = "dl_vlan_pcp";
    public static final String COLUMN_DL_TYPE = "dl_type";
    public static final String COLUMN_NW_TOS = "nw_tos";
    public static final String COLUMN_NW_PROTO = "nw_proto";
    public static final String COLUMN_NW_SRC = "nw_src"; // includes CIDR-style
                                                         // netmask, e.g.
                                                         // "128.8.128.0/24"
    public static final String COLUMN_NW_DST = "nw_dst";
    public static final String COLUMN_TP_DST = "tp_dst";
    public static final String COLUMN_TP_SRC = "tp_src";
    public static final String COLUMN_ACTIONS = "actions";
    public static final String COLUMN_BUFFER = "buffer_id";
    public static final String COLUMN_OUTPORT_LIST = "out_port_list";
    public static final String COLUMN_DELETE_ALL = "delete_all";
    public static final String COLUMN_OUTPORT = "outport";//used for deleteing all the flows with the same outport.

    public static String ColumnNames[] = { COLUMN_NAME, COLUMN_SWITCH,
            COLUMN_ACTIVE, COLUMN_IDLE_TIMEOUT, COLUMN_HARD_TIMEOUT,
            COLUMN_PRIORITY, COLUMN_COOKIE, COLUMN_WILDCARD, COLUMN_IN_PORT,
            COLUMN_DL_SRC, COLUMN_DL_DST, COLUMN_DL_VLAN, COLUMN_DL_VLAN_PCP,
            COLUMN_DL_TYPE, COLUMN_NW_TOS, COLUMN_NW_PROTO, COLUMN_NW_SRC,
            COLUMN_NW_DST, COLUMN_TP_DST, COLUMN_TP_SRC, COLUMN_ACTIONS };
 
	
    protected static IFloodlightProviderService floodlightProvider;
	
    protected IRestApiService restApi;
    
    public static void clearFlowMods(IOFSwitch sw, Short outPort) {
        // Delete all pre-existing flows with the same output action port or
        // outPort
        OFMatch match = new OFMatch().setWildcards(OFMatch.OFPFW_ALL);
        OFMessage fm = ((OFFlowMod) floodlightProvider.getOFMessageFactory()
                                                      .getMessage(OFType.FLOW_MOD)).setMatch(match)
                                                                                   .setCommand(OFFlowMod.OFPFC_DELETE)
                                                                                   .setOutPort(outPort)
                                                                                   .setLength(U16.t(OFFlowMod.MINIMUM_LENGTH));
        try {
            sw.write(fm,null);
            sw.flush();
        } catch (Exception e) {
            log.error("Failed to clear flows on switch {}", sw.getId());
        }
    }
    
    public static void getRows_install(Map<String,Object> row){
        StringBuffer matchString = new StringBuffer();
        String switchName = null;
        long dpid = 0;
        OFFlowMod flowMod = (OFFlowMod) floodlightProvider.getOFMessageFactory()
                .getMessage(OFType.FLOW_MOD);
        if (!row.containsKey(COLUMN_SWITCH)) {
            log.debug(
                    "skipping entry with missing required 'switch' entry: {}",
                    row);
            return;
        }
        try {
            // first, snag the required entries, for debugging info
            switchName = (String) row.get(COLUMN_SWITCH);
            StaticFlowEntries.initDefaultFlowMod(flowMod,"");
            if (log.isDebugEnabled()) {
                log.debug("The switch dpid is {}",switchName);
            }
            dpid = Long.parseLong(switchName);
            /*String delimiters = "[:]";
            String[] tokens = switchName.split(delimiters);
            //byte[] ret = HexString.fromHexString(switchName);
            if (tokens.length != 8){
                log.error("Invalid dpid name");
                return;
            }
            for(int i=0;i<tokens.length;i++){
                dpid = dpid+ (long)Math.pow(256, (double)(7-i))*(Integer.valueOf(tokens[i], 16));
                log.info("{}",Integer.valueOf(tokens[i], 16));
            }*/

            for (String key : row.keySet()) {
                if (row.get(key) == null){
                    continue;
                }
                if (key.equals(COLUMN_SWITCH) || key.equals(COLUMN_NAME)
                        || key.equals("id")){
                    continue; 
                }// already handled
                if (key.equals(COLUMN_HARD_TIMEOUT)){
                    flowMod.setHardTimeout((short)((long) Integer.valueOf((String)row.get(COLUMN_HARD_TIMEOUT))));
                    continue;
                }
                if (key.equals(COLUMN_IDLE_TIMEOUT)){
                    flowMod.setIdleTimeout((short)((long) Integer.valueOf((String)row.get(COLUMN_IDLE_TIMEOUT))));
                    continue;
                }
                if (key.equals(COLUMN_WILDCARD))
                    continue;
                if (key.equals(COLUMN_ACTIVE)) {
                    if  (!Boolean.valueOf((String) row.get(COLUMN_ACTIVE))) {
                        log.debug("skipping inactive entry for switch {}",
                                switchName);
                        //entries.get(switchName).put(entryName, null);  // mark this an inactive
                        return;
                    }
                } else if (key.equals(COLUMN_ACTIONS)){
                    StaticFlowEntries.parseActionString(flowMod, (String) row.get(COLUMN_ACTIONS), log);
                } else if (key.equals(COLUMN_COOKIE)) {
                    /*flowMod.setCookie(
                            StaticFlowEntries.computeEntryCookie(flowMod, 
                                    Integer.valueOf((String) row.get(COLUMN_COOKIE)), 
                                    entryName));*/
                    flowMod.setCookie(0);
                }else if(key.equals(COLUMN_OUTPORT_LIST)) {
                    continue;
                }else if (key.equals(COLUMN_PRIORITY)) {
                    flowMod.setPriority(U16.t(Integer.valueOf((String) row.get(COLUMN_PRIORITY))));
                } else { // the rest of the keys are for OFMatch().fromString()
                    if (matchString.length() > 0)
                        matchString.append(",");
                    matchString.append(key + "=" + row.get(key).toString());
                }
            }
        } catch (ClassCastException e) {
            if (switchName != null) {
                log.warn(
                        "Skipping entry on switch {} with bad data : "
                                + e.getMessage(), switchName);
            } else {
                log.warn("Skipping entry with bad data: {} :: {} ",
                        e.getMessage(), e.getStackTrace());
            }
        }

        OFMatch ofMatch = new OFMatch();
        String match = matchString.toString();
        if (log.isTraceEnabled()) {
            log.trace(match);
        }
        try {
            ofMatch.fromString(match);
        } catch (IllegalArgumentException e) {
            log.debug(
                    "ignoring flow entryon switch {} with illegal OFMatch() key: "
                            + match, switchName);
            return;
        }
        flowMod.setMatch(ofMatch);
        IOFSwitch ofSwitch = floodlightProvider.getSwitches().get(dpid);
        if (ofSwitch == null) {
            if (log.isDebugEnabled()) {
                log.debug("Not adding, switch {} not connected",
                          dpid);
            }
            return;
        }
        try {
            ofSwitch.write(flowMod, null);
            ofSwitch.flush();
        } catch (IOException e) {
            log.error("Tried to write OFFlowMod to {} but failed: {}", 
                    HexString.toHexString(ofSwitch.getId()), e.getMessage());
        }
    }

    public static void getRows_delete(Map<String,Object> row){
        StringBuffer matchString = new StringBuffer();
        String switchName = null;
        boolean delete_all = false;
        boolean delete_outport = false;
        short outport = 0;
        long dpid = 0;
        OFFlowMod flowMod = (OFFlowMod) floodlightProvider.getOFMessageFactory()
                .getMessage(OFType.FLOW_MOD);
        //flowMod.setCommand(OFFlowMod.OFPFC_DELETE_STRICT);
        if (!row.containsKey(COLUMN_SWITCH)) {
            log.debug(
                    "skipping entry with missing required 'switch' entry: {}",
                    row);
            return;
        }
        try {
            // first, snag the required entries, for debugging info
            switchName = (String) row.get(COLUMN_SWITCH);
            StaticFlowEntries.initDefaultFlowMod(flowMod,"");
            dpid = Long.parseLong(switchName);
            /*
            byte[] ret = HexString.fromHexString(switchName);
            if (ret.length != 8){
                log.error("Invalid dpid name");
                return;
            }
            for(int i=0;i<ret.length;i++){
                dpid = dpid+ (long)Math.pow(8, (double)(7-i))*ret[i];
            }*/
            if (log.isDebugEnabled()) {
                log.debug("deleting dpid:{}",dpid);
            }
            for (String key : row.keySet()) {
                if (row.get(key) == null){
                    continue;
                }
                if (key.equals(COLUMN_SWITCH) || key.equals(COLUMN_NAME)
                        || key.equals("id")){
                    continue;
                }// already handled
                if (key.equals(COLUMN_HARD_TIMEOUT))
                    continue;
                // explicitly ignore timeouts and wildcards
                /*if (key.equals(COLUMN_HARD_TIMEOUT) || key.equals(COLUMN_IDLE_TIMEOUT) ||
                        key.equals(COLUMN_WILDCARD))
                    continue;*/
                if (key.equals(COLUMN_IDLE_TIMEOUT))
                    continue;
                if (key.equals(COLUMN_WILDCARD))
                    continue;
                if (key.equals(COLUMN_ACTIVE)) {
                    if  (!Boolean.valueOf((String) row.get(COLUMN_ACTIVE))) {
                        log.debug("skipping inactive entry for switch {}",
                                switchName);
                        //entries.get(switchName).put(entryName, null);  // mark this an inactive
                        return;
                    }
                }
                else if (key.equals(COLUMN_ACTIONS)){
                    //StaticFlowEntries.parseActionString(flowMod, (String) row.get(COLUMN_ACTIONS), log);
                }
                else if (key.equals(COLUMN_DELETE_ALL)){
                    delete_all = true;
                }
                else if (key.equals(COLUMN_OUTPORT)){
                    delete_outport = true;
                    outport = (short)((long) Integer.valueOf((String)row.get(COLUMN_OUTPORT)));
                }
                else if (key.equals(COLUMN_COOKIE)) {
                    /*flowMod.setCookie(
                            StaticFlowEntries.computeEntryCookie(flowMod,
                                    Integer.valueOf((String) row.get(COLUMN_COOKIE)),
                                    entryName));*/
                    //flowMod.setCookie(0);
                } else if (key.equals(COLUMN_PRIORITY)) {
                    //flowMod.setPriority(U16.t(Integer.valueOf((String) row.get(COLUMN_PRIORITY))));
                } else { // the rest of the keys are for OFMatch().fromString()
                    if (matchString.length() > 0)
                        matchString.append(",");
                    matchString.append(key + "=" + row.get(key).toString());
                }
            }
        } catch (ClassCastException e) {
            if (switchName != null) {
                log.warn(
                        "Skipping entry on switch {} with bad data : "
                                + e.getMessage(), switchName);
            } else {
                log.warn("Skipping entry with bad data: {} :: {} ",
                        e.getMessage(), e.getStackTrace());
            }
        }

        OFMatch ofMatch = new OFMatch();
        String match = matchString.toString();
        if (log.isDebugEnabled()) {
            log.debug("{}",match);
        }
        try {
            ofMatch.fromString(match);
        } catch (IllegalArgumentException e) {
            log.debug(
                    "ignoring flow entry on switch {} with illegal OFMatch() key: "
                            + match, switchName);
            return;
        }

        OFMessage fm = ((OFFlowMod) floodlightProvider.getOFMessageFactory()
                                                      .getMessage(OFType.FLOW_MOD)).setMatch(ofMatch)
                                                                                   .setCommand(OFFlowMod.OFPFC_DELETE)
                                                                                   .setLength(U16.t(OFFlowMod.MINIMUM_LENGTH));
        IOFSwitch ofSwitch = floodlightProvider.getSwitches().get(dpid);
        if(delete_all){
            if (log.isDebugEnabled()) {
                log.debug("deleting all flows on switch {}",dpid);
            }
            ofSwitch.clearAllFlowMods();
            return;
        }
        if(delete_outport){
            if (log.isDebugEnabled()) {
                log.debug("deleting all flows for outport {} on switch {}",outport,dpid);
            }
            clearFlowMods(ofSwitch, outport);
            return;
        }

        //ofSwitch.clearAllFlowMods();
        if (ofSwitch == null) {
            if (log.isDebugEnabled()) {
                log.debug("Not deleting, switch {} not connected", 
                          dpid);
            }
            return;
        }
        try {
            ofSwitch.write(fm, null);
            ofSwitch.flush();
        } catch (IOException e) {
            log.error("Tried to write OFFlowMod to {} but failed: {}", 
                    HexString.toHexString(ofSwitch.getId()), e.getMessage());
        }
    }
    public static void getRows_packetout(Map<String,Object> row){
        OFPacketOut po = (OFPacketOut) floodlightProvider.getOFMessageFactory()
                .getMessage(OFType.PACKET_OUT);
        OFActionOutput action = new OFActionOutput();
        String switchName = null;

        if (!row.containsKey(COLUMN_SWITCH)) {
            log.debug(
                    "skipping entry with missing required 'switch'");
            return;
        }
        // most error checking done with ClassCastException
        try {
            // first, snag the required entries, for debugging info
            switchName = (String) row.get(COLUMN_SWITCH);
            long dpid = 0;
            int bufferid = 0;
            short inport = 0;
            ArrayList outportlist = new ArrayList();
            /*byte[] ret = HexString.fromHexString(switchName);
            if (ret.length != 8){
                log.error("Invalid dpid name");
                return;
            }
            for(int i=0;i<ret.length;i++){
                dpid = dpid+ (long)Math.pow(8, (double)(7-i))*ret[i];
            }*/
            dpid = Long.parseLong(switchName);
            if (log.isDebugEnabled()) {
                log.debug("pushing packet to switch:{}",dpid);
            }
            for (String key : row.keySet()) {
                if (row.get(key) == null){
                    continue;
                }
                if (key.equals(COLUMN_SWITCH) || key.equals("id")){
                    continue; 
                }// already handled
                if (key.equals(COLUMN_BUFFER)) {
                    bufferid = ((int) Integer.valueOf((String)row.get(COLUMN_BUFFER)));
                    po.setBufferId(bufferid);
                    if (log.isDebugEnabled()) {
                        log.debug("The bufferid is: {}",bufferid);
                    }
                } 
                else if (key.equals(COLUMN_IN_PORT)){
                    inport =  Short.valueOf((String)row.get(COLUMN_IN_PORT));
                    if (log.isDebugEnabled()) {
                        log.debug("The input port is {}",inport);
                    }
                    po.setInPort(inport);
                } 
                else if (key.equals(COLUMN_OUTPORT_LIST)) {
                outportlist = (ArrayList) row.get(COLUMN_OUTPORT_LIST);
                    for(int i=0;i<outportlist.size();i++){
                        Integer intport = (Integer)outportlist.get(i);
                        action.setPort(intport.shortValue());	
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("outport action is set, the number of actions are {}",outportlist.size());
                    }
                }
            }
            po.setActions(Collections.singletonList((OFAction)action));
            po.setActionsLength((short) OFActionOutput.MINIMUM_LENGTH);
            po.setLength(U16.t(OFPacketOut.MINIMUM_LENGTH
                    + po.getActionsLength()));
            log.debug("here 1");
            IOFSwitch  sw= floodlightProvider.getSwitches().get(dpid);
            try {
            	sw.write(po, null);
                sw.flush();
            } catch (IOException e) {
                log.error("Failure writing PacketOut", e);
            }
        } catch (ClassCastException e) {
            if (switchName != null) {
                log.warn(
                        "Error on switch {} with bad data : "
                                + e.getMessage(), switchName);
            } else {
                log.warn("Skipping entry with bad data: {} :: {} ",
                        e.getMessage(), e.getStackTrace());
            }
        }
    }
	
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = 
                new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IFloodlightProviderService.class);
        l.add(IRestApiService.class);
        return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider =
	            context.getServiceImpl(IFloodlightProviderService.class);
		restApi =
	            context.getServiceImpl(IRestApiService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		restApi.addRestletRoutable(new StaticFlowEntryWebRoutable());

	}

}
