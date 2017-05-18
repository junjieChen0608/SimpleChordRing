package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

/* message intent */
enum MessageIntent {
    JOIN,
    JOIN_HANDLED,
    INSERT,
    QUERY,
    DELETE,
    QUERY_ALL,
    DELETE_ALL
}

public class SimpleDhtProvider extends ContentProvider {

    public static final String TAG = "SimpleDhtProvider";
    public static final String SENDER = "Sender";
    public static final String RECEIVER = "Receiver";

    /* SQL-specific info */
    private SQLiteDatabase database;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String KEY_VALUE_TABLE = "SimpleDhtMap";

    /* other essential constants */
    private static final String JoinHandlerPort = "5554";
    private static final int SERVER_PORT = 10000;
    private static final Uri providerUri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider");
    private static final String GLOBAL = "*";
    private static final String LOCAL = "@";

    private String myPort;
    private String myHash;
    private TreeMap<String, String> theRing;

    @Override
    public boolean onCreate() {
        Log.w("Build", "6.6.2");

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myHash = genHash(myPort);
        theRing = new TreeMap<String, String>();
        theRing.put(myHash, myPort);
        Log.i(TAG, "Port: " + myPort + " , Hash: " + myHash);

        database = getContext().openOrCreateDatabase("SimpleDht.db", Context.MODE_PRIVATE, null);
        database.execSQL("DROP TABLE IF EXISTS " + KEY_VALUE_TABLE);
        database.execSQL("CREATE TABLE " + KEY_VALUE_TABLE +
                "( " + KEY_FIELD + " NOT NULL, " + VALUE_FIELD + " NOT NULL )");
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "ServerSocketException");
        }

        /* this AVD is not the 5554, so it need to join the Chord ring via 5554 */
        if (!myPort.equals(JoinHandlerPort)) {
            MessageObject msg = new MessageObject(myPort, JoinHandlerPort, null, MessageIntent.JOIN);
            msg.setData(myHash, myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }
        return database != null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Log.i(TAG, "querying: " + selection);
        Object result = queryOrDelete(MessageIntent.QUERY.name(), selection);
        return result == null ? null : (Cursor) result;
    }

    private Cursor queryLocal() {
        return database.query(KEY_VALUE_TABLE, null, null, null, null, null, null);
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString(KEY_FIELD);
        String value = values.getAsString(VALUE_FIELD);
        String targetHash = findManagingNode(genHash(key));

        Log.i(TAG, "***** Inserting *****");
        Log.i(TAG, "key: " + key);
        Log.i(TAG, "key hash: " + targetHash);
        Log.i(TAG, "my hash: " + myHash);
        Log.i(TAG, "value: " + value);

        try {
            /* if given key is managed by this AVD, then simply insert it */
            if (targetHash.equals(myHash)) {
                Log.i(TAG, "HASH MATCH");
                database.insert(KEY_VALUE_TABLE, "", values);
            } else {
                /* else forward insert request */
                MessageObject msg = new MessageObject(myPort, theRing.get(targetHash), null, MessageIntent.INSERT);
                msg.setData(key, value);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
            Log.i(TAG, "***** Inserting DONE *****");
        } catch (Exception e) {
            Log.e(TAG, "insert exception");
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        return (Integer) queryOrDelete(MessageIntent.DELETE.name(), selection);
    }

    public int deleteLocal() {
        return database.delete(KEY_VALUE_TABLE, "1", null);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    /* provided by template */
    private String genHash(String input) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Exception: ", e);
        }
        return "";
    }

    /* find the node that manages this key */
    public String findManagingNode(String hashedID) {
        int comparison = hashedID.compareTo(theRing.lastKey());
        return comparison <= 0 ? theRing.ceilingKey(hashedID) : theRing.firstKey();
    }

    /* convert arraylist to message object */
    public MessageObject parseToMsgObject(ArrayList<Object> arr) {
        return new MessageObject((String) arr.get(0), (String) arr.get(1),
                (TreeMap<String, String>) arr.get(2), (MessageIntent) arr.get(3));
    }

    /* convert cursor to treemap */
    public TreeMap<String, String> parseCursorToMap(Cursor cursor) {
        TreeMap<String, String> response = new TreeMap<String, String>();
        int keyIndex = cursor.getColumnIndex(KEY_FIELD);
        int valIndex = cursor.getColumnIndex(VALUE_FIELD);
        cursor.moveToFirst();
        while (!cursor.isAfterLast()){
            response.put(cursor.getString(keyIndex), cursor.getString(valIndex));
            cursor.moveToNext();
        }
        return response;
    }

    /* convert treemap to cursor */
    public Cursor parseMapToCursor(TreeMap<String, String> values) {
        MatrixCursor result = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
        for (String key : values.keySet()) {
            result.addRow(new String[]{key, values.get(key)});
        }
        return result;
    }

    /* refactored helper function for delete and query */
    public Object queryOrDelete(String basicIntent, String selection){
        MessageObject msg = new MessageObject();
        msg.setSenderId(myPort);
        boolean delete = basicIntent.equals(MessageIntent.DELETE.name());
        try{
            if(selection.equals(GLOBAL)){
                msg.setIntent(delete ? MessageIntent.DELETE_ALL : MessageIntent.QUERY_ALL);
                return new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
            }else if(selection.equals(LOCAL)){
                return delete ? deleteLocal() : queryLocal();
            }else {
                String targetHash = findManagingNode(genHash(selection));
                if(targetHash.equals(myHash)){
                    return delete ? database.delete(KEY_VALUE_TABLE, KEY_FIELD + "=?", new String[]{selection}) :
                                    database.query(KEY_VALUE_TABLE, null, KEY_FIELD + "=?", new String[]{selection}, null, null, null);
                }else {
                    msg.setReceiverId(theRing.get(targetHash));
                    msg.setIntent(delete ? MessageIntent.DELETE : MessageIntent.QUERY);
                    msg.setData(delete ? MessageIntent.DELETE.name() : MessageIntent.QUERY.name(), selection);
                    return new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
                }
            }
        }catch (Exception e){
            Log.e(TAG, "query or delete exception");
            e.printStackTrace();
        }
        return delete ? 0 : null;
    }

    /* Server side */
    private class ServerTask extends AsyncTask<ServerSocket, MessageObject, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket client = serverSocket.accept();
                    ObjectOutputStream writer = new ObjectOutputStream(client.getOutputStream());
                    ObjectInputStream reader = new ObjectInputStream(client.getInputStream());
                    MessageObject msgReceived = parseToMsgObject(((ArrayList<Object>) reader.readObject()));
                    MessageIntent msgIntent = msgReceived.getIntent();
                    String target;
                    String firstKey;
                    switch (msgIntent) {
                        case JOIN:
                            Log.i(RECEIVER, "Handle join request from " + msgReceived.getSenderId());
                            firstKey = msgReceived.getData().firstKey();
                            theRing.put(firstKey, msgReceived.getData().get(firstKey));
                            MessageObject msg = new MessageObject(myPort, "", theRing, MessageIntent.JOIN_HANDLED);
                            writeHelper(writer, null);
                            publishProgress(msg);
                            break;
                        case JOIN_HANDLED:
                            theRing = msgReceived.getData();
                            writeHelper(writer, null);
                            break;
                        case INSERT:
                            firstKey = msgReceived.getData().firstKey();
                            ContentValues values = new ContentValues();
                            values.put(KEY_FIELD, firstKey);
                            values.put(VALUE_FIELD, msgReceived.getData().get(firstKey));
                            insert(providerUri, values);
                            writeHelper(writer, null);
                            break;
                        case QUERY:
                            target = msgReceived.getData().get(msgIntent.name());
                            Cursor cursor = query(providerUri, null, target, null, null);
                            writeHelper(writer, parseCursorToMap(cursor));
                            break;
                        case DELETE:
                            target = msgReceived.getData().get(msgIntent.name());
                            writeHelper(writer, delete(providerUri, target, null));
                            break;
                        case QUERY_ALL:
                            writeHelper(writer, parseCursorToMap(queryLocal()));
                            break;
                        case DELETE_ALL:
                            writeHelper(writer, deleteLocal());
                            break;
                    }
                    writer.close();
                    reader.close();
                    client.close();
                } catch (Exception e) {
                    Log.e(TAG, "Server side exception");
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(MessageObject... msgs) {
            Log.i(RECEIVER, msgs[0].getSenderId() + " try to send msg to " + msgs[0].getReceiverId());
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[0]);
        }

        private void writeHelper(ObjectOutputStream writer, Object toWrite) throws IOException {
            writer.writeObject(toWrite);
            writer.flush();
        }
    }

    /* Client side */
    private class ClientTask extends AsyncTask<MessageObject, Void, Object> {

        @Override
        protected Object doInBackground(MessageObject... msgs) {
            MessageObject msg = msgs[0];
            switch (msg.getIntent()) {
                case JOIN:
                case INSERT:
                    unicast(msg.parseToArrayList());
                    break;
                case QUERY:
                    return parseMapToCursor( (TreeMap<String, String>) unicast(msg.parseToArrayList()) );
                case DELETE:
                    return unicast(msg.parseToArrayList());
                case QUERY_ALL:
                    return parseMapToCursor((TreeMap<String, String>) globalOperationHelper(msg));
                case DELETE_ALL:
                    return globalOperationHelper(msg);
                case JOIN_HANDLED:
                    for (String remotePort : msg.getData().values()) {
                        if(!remotePort.equals(JoinHandlerPort)){
                            msg.setReceiverId(remotePort);
                            unicast(msg.parseToArrayList());
                        }
                    }
                    break;
            }
            return null;
        }

        private Object globalOperationHelper(MessageObject msg){
            boolean deleteAll = msg.getIntent().equals(MessageIntent.DELETE_ALL);
            TreeMap<String, String> data = new TreeMap<String, String>();
            int counter = 0;
            for (String remotePort : theRing.values()){
                if(!remotePort.equals(myPort)){
                    msg.setReceiverId(remotePort);
                    if(deleteAll){
                        counter += (Integer) unicast(msg.parseToArrayList());
                    }else {
                        data.putAll((TreeMap<String, String>)unicast(msg.parseToArrayList()));
                    }
                }else {
                    if(deleteAll){
                        counter += deleteLocal();
                    }else {
                        data.putAll(parseCursorToMap(queryLocal()));
                    }
                }
            }
            return deleteAll ? counter : data;
        }

        private Object unicast(ArrayList<Object> msg) {
            Object response = null;
            try {
                int remotePort = Integer.parseInt((String) msg.get(1)) * 2;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remotePort);
                ObjectOutputStream writer = new ObjectOutputStream(socket.getOutputStream());
                Log.i(SENDER, "From " + msg.get(0) + " to " + msg.get(1));
                writer.writeObject(msg);
                writer.flush();
                ObjectInputStream reader = new ObjectInputStream(socket.getInputStream());
                response = reader.readObject();
                writer.close();
                reader.close();
                socket.close();
            } catch (Exception e) {
                Log.e(TAG, "Client side exception");
                e.printStackTrace();
            }
            return response;
        }
    }
}
