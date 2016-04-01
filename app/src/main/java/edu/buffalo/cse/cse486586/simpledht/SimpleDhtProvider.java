package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    static final int HEAD_PORT = 5554 * 2;
    static int prevNode;
    static int nextNode;
    private int myPort;
    private String nodeID;
    public static String TAG = "DHTProvider";
    static Context mContext = null;

    private static String JOIN_REQUEST = "JOINNOW!";
    private static String JOIN_ACCEPT = "Joined";
    private static String PREV_UPDATE = "Prev_Update";
    private static String INSERT_KEY = "InsertKey";
    private static String QUERY_KEY = "QueryKey";
    private static String QUERY_RESULT = "QueryResult";
    private static String QUERY_ALL = "QueryAll";

    private static String delim = "`";
    Semaphore waitForQuery = new Semaphore(0);
    boolean flag = false;
    private String queryResult;

    public SimpleDhtProvider() {
        super();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Set<Map.Entry<String, Object>> contentSet = values.valueSet();
        String args[] = new String[2];  //args[0] value, args[1] key
        Iterator start = contentSet.iterator();
        int i = 0;
        while (start.hasNext()) {
            Map.Entry<String, Object> keypair = (Map.Entry<String, Object>) start.next();
            args[i++] = (String) keypair.getValue();
        }
        Log.v("insert", values.toString());

        String insDht = INSERT_KEY + delim + args[1] + delim + args[0];
//        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, insDht, String.valueOf(myPort));
        sendMessage(insDht, String.valueOf(myPort));


        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        //TODO telephony code in Activity??
        //TODO server task initialization in activity?
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        Log.v(TAG, tel.getLine1Number());
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = (Integer.parseInt(portStr) * 2);
        nodeID = genHash(String.valueOf(myPort / 2));
        Log.v(TAG, "PORT: " + myPort);

        prevNode = nextNode = myPort;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
        }

        if (myPort == HEAD_PORT) {
            Log.v(TAG, "at 5554");
            return false;
        }
        String joinRequest = JOIN_REQUEST + delim + genHash(String.valueOf(myPort / 2)) + delim + myPort;
        Log.v(TAG, joinRequest);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinRequest, String.valueOf(HEAD_PORT));

        //TODO join, send request to 5554

        //TODO wait for response?
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        MatrixHelper cursorBuilder = null;
        if (selection.equals("@")) {
            Log.v("DHTQuery", "Local dump");
            cursorBuilder = new MatrixHelper(localQueryAll());
        } else if (selection.equals("*")){
            String queryAllDht = QUERY_ALL + delim + myPort;
            sendMessage(queryAllDht, String.valueOf(myPort));
            try {
                waitForQuery.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cursorBuilder = new MatrixHelper(queryResult);
        }
        else {
            String queryDht = QUERY_KEY + delim + selection + delim + myPort;
            sendMessage(queryDht, String.valueOf(myPort));
            Log.v("Query Result", queryDht);
            try {
                waitForQuery.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Log.v("QueryResult", queryResult + " " + selection);
            if (queryResult == null)
                return null;
            cursorBuilder = new MatrixHelper(selection + delim + queryResult);
        }
        //TODO add more rows

        return cursorBuilder.cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    private String genHash(String input) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            do {
                try {
                    Socket clientHook = serverSocket.accept();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientHook.getInputStream()));
                    String message = reader.readLine();
                    Log.v(TAG, "Server " + message);
                    String[] args = message.split(delim);

                    if (args[0].equals(JOIN_REQUEST)) {
                        addNodeToRing(args[1], args[2]);
                    } else if (args[0].equals(JOIN_ACCEPT)) {
                        prevNode = Integer.parseInt(args[1]);
                        nextNode = Integer.parseInt(args[2]);
                    } else if (args[0].equals(PREV_UPDATE)) {
                        prevNode = Integer.parseInt(args[1]);
                    } else if (args[0].equals(INSERT_KEY)) {
                        handleInsert(args[1], args[2]);
                    } else if (args[0].equals(QUERY_KEY)) {
                        queryKey(args[1], args[2]);
                    } else if (args[0].equals(QUERY_ALL)){
                        queryAll(message);
                    } else if (args[0].equals(QUERY_RESULT)) {
                        message = message.substring(message.indexOf(delim)+1);
                        queryResult = message;
                        waitForQuery.release();
                        flag = true;
                    }
                    //TODO received some message change to serializable object?
                    clientHook.close();
                    //serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            } while (true);
        }

        protected void onProgressUpdate(String... strings) {
            //Nothin to do here
        }
    }

    private void queryAll(String message) {
        String[] args = message.split(delim);
        if(nextNode == prevNode){
            String result = QUERY_RESULT + delim + localQueryAll();
            sendMessage(result, args[1]);
            return;
        }
        String localValues = localQueryAll();

        if(localValues!=null)
           message = message + delim + localValues.substring(0, localValues.lastIndexOf(delim));

        if(nextNode == Integer.parseInt(args[1])) {
            int cutString = message.indexOf(delim, message.indexOf(delim)+1);
            message = QUERY_RESULT + message.substring(cutString);
        }
        sendMessage(message, String.valueOf(nextNode));
    }

    private void queryKey(String key, String port) {
        String hash = genHash(key);
        if (nextNode == prevNode) {
            localQuery(key, port);
            return;
        }
//        String nextID = genHash(String.valueOf(nextNode/2));
        String prevID = genHash(String.valueOf(prevNode / 2));
        if (greaterThan(hash, prevID) && !greaterThan(hash, nodeID)) {
            Log.v("Query", "Query found");
            localQuery(key, port);
        } else if (!greaterThan(nodeID, prevID)) {
            if ((!greaterThan(hash, nodeID) && greaterThan(prevID, hash))
                    || (greaterThan(hash, prevID)) && greaterThan(hash, nodeID)) {
                //local query
                localQuery(key, port);
            } else {
                String insDht = QUERY_KEY + delim + key + delim + port;
                sendMessage(insDht, String.valueOf(nextNode));
            }
        } else {
            String insDht = QUERY_KEY + delim + key + delim + port;
            sendMessage(insDht, String.valueOf(nextNode));
        }
    }

    private void localQuery(String key, String port) {
        FileInputStream key_retrieve = null;
        try {
            String message;
            key_retrieve = getContext().openFileInput(key);
            if (key_retrieve == null)
                message = null;
            else {
                BufferedReader buf = new BufferedReader(new InputStreamReader(key_retrieve));
                message = buf.readLine();
            }
            Log.v("query", "key " + key + " value " + message);
            String queryResult = QUERY_RESULT + delim + message;
            sendMessage(queryResult, String.valueOf(port));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String localQueryAll() {
        File dir = getContext().getFilesDir();
        String[] files = dir.list();
        String queryAll = null;
        FileInputStream key_retrieve;
        String message;

        for (String file : files) {
            Log.v("LocalQuery", file);
            try {
                key_retrieve = getContext().openFileInput(file);
                if (key_retrieve == null)
                    message = null;
                else {
                    BufferedReader buf = new BufferedReader(new InputStreamReader(key_retrieve));
                    message = buf.readLine();
//                    queryAll.addRow(file, message);
                    if(queryAll == null)
                        queryAll = file + delim + message + delim;
                    else
                        queryAll = queryAll + file + delim + message + delim;
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        Log.v("QueryAll", queryAll);
        return queryAll;
    }

    private void handleInsert(String key, String value) {
        String hash = genHash(key);
//        String nextID = genHash(String.valueOf(nextNode / 2));
        String prevID = genHash(String.valueOf(prevNode / 2));
        if (greaterThan(hash, prevID) && !greaterThan(hash, nodeID)) {
            Log.v("Insert", "Insert in node");
            localInsert(key, value);
        } else if (!greaterThan(nodeID, prevID)) {
            //Double negatives!!
            if ((!greaterThan(hash, nodeID) && greaterThan(prevID, hash))
                    || (greaterThan(hash, prevID)) && greaterThan(hash, nodeID)) {
                localInsert(key, value);
            } else {
                String insDht = INSERT_KEY + delim + key + delim + value;
                sendMessage(insDht, String.valueOf(nextNode));
            }
        } else {
            String insDht = INSERT_KEY + delim + key + delim + value;
            sendMessage(insDht, String.valueOf(nextNode));
        }
    }

    private void localInsert(String key, String value) {
        Log.v("Insert", key + " " + value);
        FileOutputStream key_store = null;
        try {
            key_store = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            key_store.write(value.getBytes());
            key_store.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addNodeToRing(String hash, String port) {
        if (prevNode == myPort && nextNode == myPort) {
            //Nothing in the ring, two can form a ring
            String joined = JOIN_ACCEPT + delim + prevNode + delim + nextNode;
            Log.v(TAG, "Initial Addition");
            prevNode = Integer.parseInt(port);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joined, port);
            nextNode = prevNode;
            return;
        }
        String nextID = genHash(String.valueOf(nextNode / 2));
        if (greaterThan(hash, nodeID) && !greaterThan(hash, nextID)) {
            Log.v(TAG, "Added " + port + " " + hash);
            String joined = JOIN_ACCEPT + delim + myPort + delim + nextNode;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joined, String.valueOf(port));
            String updateSuccessor = PREV_UPDATE + delim + port;
            Log.v(TAG, "Predecessor update" + port);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, updateSuccessor, String.valueOf(nextNode));
            nextNode = Integer.parseInt(port);
            nextNode = Integer.parseInt(port);
        } else if ((greaterThan(hash, nodeID) &&
                greaterThan(hash, nextID)) || !greaterThan(hash, nodeID)) {
            if (greaterThan(nextID, nodeID)) {
                Log.v(TAG, "Pass along");
                String joinRequest = JOIN_REQUEST + delim + hash + delim + port;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinRequest, String.valueOf(nextNode));
            } else {
                Log.v(TAG, "Accept connection " + port);
                String joined = JOIN_ACCEPT + delim + myPort + delim + nextNode;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joined, String.valueOf(port));
                String updateSuccessor = PREV_UPDATE + delim + port;
                Log.v(TAG, "Predecessor update" + port);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, updateSuccessor, String.valueOf(nextNode));
                nextNode = Integer.parseInt(port);
            }
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket join = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]));
                Log.v(TAG, "Client sends: " + msgs[0] + " to " + msgs[1]);
                OutputStream out = (join.getOutputStream());
                byte[] byteStream = msgs[0].getBytes("UTF-8");
//                Log.e(TAG, "Sending bytes " +byteStream);
                out.write(byteStream);
                join.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private void sendMessage(String message, String port) {
        try {
            Socket join = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            Log.v(TAG, "Client sends: " + message + " to " + port);
            OutputStream out = (join.getOutputStream());
            byte[] byteStream = message.getBytes("UTF-8");
//                Log.e(TAG, "Sending bytes " +byteStream);
            out.write(byteStream);
            join.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean greaterThan(String first, String second) {
        //Returns true if first is greater than second
        return (first.compareTo(second) > 0);
    }

}
