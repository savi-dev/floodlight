package net.floodlightcontroller.floodlight2janus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

/***
 * 
 * @author ke email: klwangke@163.com
 *
 */
public class RestcallThread implements Runnable{

	/**
	 * @param args
	 */
	public static int TYPEPOST = 0;
	public static int TYPEPUT = 1;
	
	Queue<StringEntity> restqueue = new LinkedList<StringEntity> ();
	String janusiphost = null;
	int type = 0;
	public RestcallThread(LinkedList<StringEntity> queue, String iphost,int httptype){
		restqueue = queue;
		janusiphost = iphost;
		type = httptype;
		System.out.println(restqueue.size());
	}
	public void run(){
		HttpClient httpclient = new DefaultHttpClient();
		HttpPut httpput = new HttpPut("http://"+janusiphost+"/v1.0/events/0");
		HttpPost httppost = new HttpPost("http://"+janusiphost+"/v1.0/events/0");
		while(true){
			if(type==1){
			    while(!Floodlight2Janus.putqueue.isEmpty()){
                    httpput.setEntity(Floodlight2Janus.putqueue.poll());
                    HttpResponse response;
                    try {
                        response = httpclient.execute(httpput);
                        HttpEntity entity = response.getEntity();
                        httpput.releaseConnection();
                        if (entity != null) {
                            InputStream instream = httpput.getEntity().getContent();
                            BufferedReader in = new BufferedReader(new InputStreamReader(instream));
                            try {
                                String line;
                                while((line = in.readLine())!=null){
                                    System.out.println(line);
                                }
                                System.out.println(response.getStatusLine());
                            } finally {
                                instream.close();
                            }
                        }
                    } catch (ClientProtocolException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
			     }
			}
			else if(type==0){
			    while(!Floodlight2Janus.postqueue.isEmpty()){
                    httppost.setEntity(Floodlight2Janus.postqueue.poll());
                    HttpResponse response;
                    try {
                        response = httpclient.execute(httppost);
                        HttpEntity entity = response.getEntity();
                        httppost.releaseConnection();
                        if (entity != null) {
                            InputStream instream = httppost.getEntity().getContent();
                            BufferedReader in = new BufferedReader(new InputStreamReader(instream));
                            try {
                                String line;
                                while((line = in.readLine())!=null){
                                    System.out.println(line);
                                }
                                System.out.println(response.getStatusLine());
                            } finally {
                                instream.close();
                            }
                        }
                    } catch (ClientProtocolException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                 }
			}
		}
	}

}
