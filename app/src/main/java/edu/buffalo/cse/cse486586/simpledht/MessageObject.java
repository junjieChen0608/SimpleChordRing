package edu.buffalo.cse.cse486586.simpledht;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

public class MessageObject {

    String senderId;
    String receiverId;
    TreeMap<String,String> data;
    MessageIntent msgIntent;

    /* default constructor */
    public MessageObject(){}

    /* alternative constructor */
    public MessageObject(String sndId, String rcvId, TreeMap<String, String> map, MessageIntent intent){
        senderId = sndId;
        receiverId = rcvId;
        data = map;
        msgIntent = intent;
    }

    public void setSenderId(String toSet){ senderId = toSet; }
    public String getSenderId(){ return senderId; }

    public void setReceiverId(String toSet){ receiverId = toSet; }
    public String getReceiverId(){ return receiverId; }

    public void setIntent(MessageIntent intent){ msgIntent = intent; }
    public MessageIntent getIntent(){ return msgIntent; }

    public void setData(String key, String val){
        if(data == null){ data = new TreeMap<String, String>(); }
        data.put(key, val);
    }

    public void setData(TreeMap<String, String> toSet){ data = toSet; }

    public TreeMap<String, String> getData(){ return data; }

    public ArrayList<Object> parseToArrayList(){
        /*
        * 0 senderId
        * 1 receiverId
        * 2 data map
        * 3 intent
        * */
        return new ArrayList<Object>(Arrays.asList(senderId, receiverId, data, msgIntent));
    }
}
