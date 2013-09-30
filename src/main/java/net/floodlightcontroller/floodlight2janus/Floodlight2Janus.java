package net.floodlightcontroller.floodlight2janus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.openflow.protocol.OFFeaturesReply;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;

import org.openflow.protocol.OFPortStatus;
import org.openflow.protocol.OFPortStatus.OFPortReason;
import org.openflow.protocol.OFType;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;


import java.util.ArrayList;

import net.floodlightcontroller.packet.Ethernet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Floodlight2Janus implements IOFMessageListener, IFloodlightModule {

    protected IFloodlightProviderService floodlightProvider;
    protected static Logger logger;
    private String janusiphost = null;
    static LinkedList<StringEntity> postqueue = new LinkedList<StringEntity> ();
    static LinkedList<StringEntity> putqueue = new LinkedList<StringEntity> ();
    @Override
    public String getName() {
        return "Floodlight2Janus";
    }

    @Override
    public boolean isCallbackOrderingPrereq(OFType type, String name) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isCallbackOrderingPostreq(OFType type, String name) {
        // TODO Auto-generated method stub
        return false;
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
        Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IFloodlightProviderService.class);
        return l;
    }

    @Override
    public void init(FloodlightModuleContext context)
            throws FloodlightModuleException {
        floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
        logger = LoggerFactory.getLogger(Floodlight2Janus.class);
        Map<String, String> configOptions = context.getConfigParams(this);
        try {
            janusiphost = configOptions.get("janusiphost");
            logger.info("setting janus ip host pair to: {}",janusiphost);
        } catch (NumberFormatException e) {
            logger.warn("Reading configuration file error.");
        }
    }

    @Override
    public void startUp(FloodlightModuleContext context) {
        floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
        floodlightProvider.addOFMessageListener(OFType.PORT_STATUS, this);
        floodlightProvider.addOFMessageListener(OFType.FEATURES_REPLY, this);
        
        RestcallThread restpostthread1 = new RestcallThread(postqueue,janusiphost,0);
        RestcallThread restputthread1 = new RestcallThread(putqueue,janusiphost,1);
        
        Thread postthread1 = new Thread(restpostthread1,"postthread1");
        Thread putthread1 = new Thread(restputthread1,"putthread1");
        
        postthread1.start();
        putthread1.start();
    }

    @Override
    public net.floodlightcontroller.core.IListener.Command receive(
            IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
        
        HttpClient httpclient = new DefaultHttpClient();
        
         switch(msg.getType()) {
         case PACKET_IN:
             if (logger.isDebugEnabled()) {
                 logger.debug("PACKET_IN received from switch {}",sw.getId());
             }
             Ethernet eth =
                IFloodlightProviderService.bcStore.get(cntx,
                                         IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
             
             OFPacketIn pi = (OFPacketIn) msg;
             String str = eth.getSourceMAC().toString();
             HttpPost httppost = new HttpPost("http://"+janusiphost+"/v1.0/events/0");
             
             try {
                StringEntity input = new StringEntity("{\"event\":{\"in_port\":"+pi.getInPort()+",\"eth_type\":"+eth.getEtherType()
                         +",\"datapath_id\":"+sw.getId()+",\"buffer_id\":"+pi.getBufferId()+",\"of_event_id\":"+1+",\"dl_src\":\""
                        +eth.getSourceMAC()+"\",\"dl_dst\":\""+eth.getDestinationMAC()+"\"}}");
                input.setContentType("application/json");
                postqueue.offer(input);
            } catch (UnsupportedEncodingException e1) {
                e1.printStackTrace();
            }
             
             break;
         case PORT_STATUS:
             OFPortStatus portstatus = (OFPortStatus) msg;
             byte reason = portstatus.getReason();
             short port_no = portstatus.getDesc().getPortNumber();
             int reason_id;
             boolean flag = false;
             if(reason==(byte)OFPortReason.OFPPR_ADD.ordinal()){
                 if (logger.isDebugEnabled()) {
                     logger.debug("port added {}",port_no);
                 }
                 flag = true;
                 reason_id = 0;
             }
             else if(reason==(byte)OFPortReason.OFPPR_DELETE.ordinal()){
                 if (logger.isDebugEnabled()){
                     logger.debug("port deleted {}",port_no);
                 }
                 flag = false;
                 reason_id = 1;
             }
             else if(reason==(byte)OFPortReason.OFPPR_MODIFY.ordinal()){
                 if (logger.isDebugEnabled()) {
                     logger.debug("port modified {}",port_no);
                 }
                 flag = false;
                 reason_id = 2;
             }
             else{
                 if (logger.isDebugEnabled()) {
                     logger.debug("Illegal port state. Return from error");
                 }
                 return Command.CONTINUE;
             }
             StringEntity portstatusinput;
             try {
                 portstatusinput = new StringEntity("{\"event\":{\"port\":"+port_no+
                            ",\"datapath_id\":"+sw.getId()+",\"reason\":"+reason_id+",\"of_event_id\":"+3+"}}");
                
            portstatusinput.setContentType("application/json");
            HttpResponse portresponse = null;
            if (logger.isDebugEnabled()) {
                logger.debug("Fowarding PORT STATUS to JANUS: {}",portstatusinput);
            }
            if(flag){
			    putqueue.offer(portstatusinput);
			}
			else{
			    postqueue.offer(portstatusinput);
			}
             } catch (UnsupportedEncodingException e1) {
                 // TODO Auto-generated catch block
                 e1.printStackTrace();
             }
             
             break;
         case FEATURES_REPLY:
             OFFeaturesReply featreply = (OFFeaturesReply) msg;
             HttpPut httpput = new HttpPut("http://"+janusiphost+"/v1.0/events/0");
             HttpResponse featresponse;
             logger.info("preparing to send feature's reply.");
             StringEntity featinput;
            try {
                int ports_no = featreply.getPorts().size();
                ArrayList activeports = new ArrayList<>();
                for(int i=0;i<ports_no;i++){
                    activeports.add((featreply.getPorts().get(i).getPortNumberlong()&0x0FFFF));
                }
                featinput = new StringEntity("{\"event\":{\"ports\":"+activeports+
                        ",\"datapath_id\":"+featreply.getDatapathId()+",\"of_event_id\":"+0+"}}");
            featinput.setContentType("application/json");
            putqueue.offer(featinput);
            logger.info("{}",putqueue.size());
            if (logger.isDebugEnabled()) {
                logger.debug("Fowarding FEATURES REPLY to JANUS: {}","{\"event\":{\"ports\":"+activeports+
                    ",\"datapath_id\":"+featreply.getDatapathId()+",\"of_event_id\":"+0+"}}");
            }
            } catch (UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
             break;
         default:
             if (logger.isDebugEnabled()) {
                 logger.debug("{} received from switch {}",msg.getType(),sw.getId());
             }
             break;
     }
     return Command.CONTINUE;
    }

}
