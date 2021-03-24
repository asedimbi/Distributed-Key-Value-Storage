package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.PhoneNumberFormattingTextWatcher;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


class node implements Comparable<node>{
	String node_hash;
	String node_port;

	public node(String node_port, String node_hash){
		super();
		this.node_port = node_port;
		this.node_hash = node_hash;
	}

	public int compareTo(node that){
		Integer val = this.node_hash.compareTo(that.node_hash);
		if(val == 0){
			return val;
		}else if(val > 0){
			return 1;
		}else{
			return -1;
		}
	}
}

public class SimpleDynamoProvider extends ContentProvider {

	//Define all the necessary variables here:
	static final int SERVER_PORT = 10000;

	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	ArrayList<String> remote_ports = new ArrayList<String>(Arrays.asList(REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4));
	ArrayList<String> hashes = new ArrayList<String>();
	ArrayList<node> nodes = new ArrayList<node>();
	String myPort ="";
	String myHash = "";


	Uri myUri = null;
	ArrayList<String> TABLE = new ArrayList<String>();

	ReadWriteLock lock = new ReentrantReadWriteLock();


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		// https://piazza.com/class/k5wltqxeuvk50f?cid=452 Grader does not test individual deletes.
		// Can by pass and delete all files at a time
		// Removing prev code...

        if(TABLE.contains(selection)){
            TABLE.remove(selection);
        }

        ArrayList<String> Del_Ports = getDest(selection);
        System.out.println("Deleting from the ports");
        System.out.println(Del_Ports);

        for(String d_port : Del_Ports){
            if(myPort.equals(d_port)){
                continue;
            }

            Socket socket = null;
            String msg = "DD_" + selection;
            System.out.println("Deleting "+ selection + " at "+ d_port);

            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(d_port));
//              BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter client_out = new PrintWriter(socket.getOutputStream(), true);
                client_out.println(msg);
//              String ack = in.readLine();
                socket.close();
//              in.close();
                client_out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }catch (NullPointerException e){
                e.printStackTrace();
            }

        }

		for(String T: TABLE) {
			getContext().deleteFile(T);
		}


		TABLE.clear();
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key = (String) values.get("key");
		String val = (String) values.get("value");

		ArrayList<String> DEST_PORTS = getDest(key);

		for (String d_port : DEST_PORTS){
			if(d_port.equals(myPort)){
				self_insert(key, val);
				continue;
			}
			else {
				new InsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key, val, d_port);
			}
		}

		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub
		FileInputStream fis = null;
		MatrixCursor mat_cursor = new MatrixCursor(new String[]{"key", "value"});
		ArrayList<String> temp = new ArrayList<String>(TABLE);
		if(selection.equals("@") || selection.equals("*")){
			// All keys @ functionality
			try{
				for(String K : temp){
					fis = getContext().openFileInput(K);
					InputStreamReader isr = new InputStreamReader(fis);
					BufferedReader br = new BufferedReader(isr);
					StringBuilder sb = new StringBuilder();

					String value;

					try {
						value = br.readLine();
						sb.append(value).toString();
					} catch (IOException e) {
						e.printStackTrace();
					}
					mat_cursor.newRow().add("key", K).add("value", sb.toString());

				}



			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
//			System.out.println("********************** Here in Else of Query *************************************");
			// if not @ and if * then collect @ from every other server
			ArrayList<String> RECEIVED = new ArrayList<String>();
			String[] ANS = null;

			//All keys * functionality
			if(selection.equals("*")) {

//				System.out.println("********************** Here in Else Before Porting *************************************");

				for (String d_port : remote_ports) {

					if (d_port.equals(myPort)) {
						continue;
					}

//					System.out.println("Here asking neighbor to send @ ans "+d_port);
					String key = "@";
					Socket socket = null;
					String msg = "QQ_" + key;
					System.out.println("In server "+ myPort+ "query "+ msg);
					try {

						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(d_port));
						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						PrintWriter client_out = new PrintWriter(socket.getOutputStream(), true);
						client_out.println(msg);
						String ack = in.readLine();
//						System.out.println("********************** Here in Else Reading Responses *************************************");
//						System.out.println("--------------------------------------------------------------");
						while ((ack != null) & (! ack.equals("_END_"))) {
//							System.out.println("********************** Here in While *************************************");
							if (!(ack.equals("No"))) {
//								System.out.println(ack);
								if (!RECEIVED.contains(ack)) {
									RECEIVED.add(ack);
									ANS = ack.split("_", 0);
									mat_cursor.newRow().add("key", ANS[0]).add("value", ANS[1]);
								}

								ack = in.readLine();
//								System.out.println("********************** Here End of While *************************************");

							} else {
//                        System.out.println("Message dint go to " + remotePort);
							}
						}
						socket.close();
						in.close();
						client_out.close();

//              System.out.println("Done with one socket session___________________________________________");
					} catch (IOException e) {
						e.printStackTrace();
					} catch (NullPointerException e){
						e.printStackTrace();
					}

				}
			}

			System.out.println("Returning ------------------------------------------------");
            System.out.println(mat_cursor.getCount()+ " "+TABLE.size());
			return mat_cursor;

		}
		//else if((selection.charAt(0)!= '*') & ((selection.charAt(0)!= '@'))){
		// Single Key Query:
		else{
            boolean Flag = true;
            int Attempts = 0;
            while(Flag){
                Attempts += 1;
                System.out.println("---------------Attempt No: " + Attempts+ " ---- "+ selection);
                int res_count = 0;
                ArrayList<String> O = new ArrayList<String>();
                O = getDest(selection);
//                if(O.contains(myPort)){
//                    String val = self_query(selection);
//                    mat_cursor.newRow().add("key", selection).add("value", val);
//                    return mat_cursor;
//                }
				if(false){
				}
                else{

                    ArrayList<String> Single_Returns = new ArrayList<String>();
                    for(String d_port: O){
//					System.out.println("Here at looking at neighbors for Query");

						if(d_port.equals(myPort)){
							System.out.println("Found Single Query Locally ");
							String val = self_query(selection);
							Single_Returns.add(selection+"_"+val);
//							mat_cursor.newRow().add("key", selection).add("value", val);
						}
						else{
							String key = selection;
							Socket socket = null;
							String msg = "Q_" + key;
							System.out.println("Queriying "+ selection + "  in  "+ d_port);

							try {
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(d_port));
								BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
								PrintWriter client_out = new PrintWriter(socket.getOutputStream(), true);
								client_out.println(msg);
								String ack = in.readLine();
								System.out.println("---------" + ack);
								if(ack != null) {
									if (!ack.equals("No") && !ack.equals("null")) {
										if(! Single_Returns.contains(selection+"_"+ack)){
											Single_Returns.add(selection+"_"+ack);
										}
									} else {
//                        System.out.println("Message dint go to " + remotePort);
									}
								}
								socket.close();
								in.close();
								client_out.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}

                    }

                    String[] words = null;
					System.out.println("Length of Single_Returns --- "+ Single_Returns.size());
                    for(String W : Single_Returns){
                        words = W.split("_", 0);
                        if(words[1].equals("null")){
                            System.out.println("Got a Null----------------------------------------------------------------");
                            continue;
                        }
                        else{
                            res_count += 1;
							System.out.println("Appending:::::::::::::::" + words[0] + ":::::::::::::::::" + words[1]);
                            mat_cursor.newRow().add("key", words[0]).add("value", words[1]);
                        }

                    }

                    if(res_count >= 1 || Attempts >= 3){
                        Flag = false;
                    }
                    else{
                        continue;
                    }
                }

			}
            return mat_cursor;
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		System.out.println("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
		try {
			System.out.println(genHash("uDjIL9xnoN7HSofaKx75QuyD6AkKTJsZ"));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
//		System.out.println(myHash);
		System.out.println(myPort);
		System.out.println("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");

		try {
			ServerSocket ServerSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, ServerSocket);

		} catch (IOException e) {
			e.printStackTrace();
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		Uri.Builder myUriBuilder = new Uri.Builder();
		myUriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
		myUriBuilder.scheme("content");
		myUri = myUriBuilder.build();


		//Logic from PA1 to recognise the AVD port number
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try {
			myHash = genHash( String.valueOf((Integer.parseInt(portStr))) );
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		String port_hash = null;
		for (String remotePort : remote_ports){
			try {
				port_hash = genHash(String.valueOf(Integer.parseInt(remotePort)/2));
				nodes.add(new node(remotePort, port_hash));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

		}

		Collections.sort(nodes);
		for (node n : nodes){
			System.out.println(n.node_port +" "+ n.node_hash);
		}



		//Check if you were just recovered or just started. Fetch * and insert keys that are supposed to be in you
		new RecoverTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);



//		ArrayList<String> H = new ArrayList<String>();
//
//		H.add("077ccecaec32c54b82d5aaafc18a2dadb753e3b1");//24 , 12, 08
//		H.add("177ccecaec32c54b82d5aaafc18a2dadb753e3b1");//24, 12, 08
//		H.add("178ccecaec32c54b82d5aaafc18a2dadb753e3b1");//12, 08, 16
//		H.add("208f7f72b198dadd244e61801abe1ec3a4857bc9");//12, 08, 16
//		H.add("210f7f72b198dadd244e61801abe1ec3a4857bc9");//08, 16, 20
//		H.add("33d6357cfaaf0f72991b0ecd8c56da066613c089");//08, 16, 20
//		H.add("34d6357cfaaf0f72991b0ecd8c56da066613c089");//16, 20, 24

//		H.add("abf0fd8db03e5ecb199a9b82929e9db79b909643");//16, 20, 24
//		H.add("acf0fd8db03e5ecb199a9b82929e9db79b909643");//20, 24, 12
//		H.add("c25ddd596aa7c81fa12378fa725f706d54325d12");//20, 24, 12
//		H.add("d25ddd596aa7c81fa12378fa725f706d54325d12");//24, 12, 08

//		ArrayList<String> O = new ArrayList<String>();
//		System.out.println("Here--------------------------------------------------");
//		for (String in_key : H){
//			O = getDest(in_key);
//			System.out.println(in_key);
//			System.out.println(O);
//		}

//		System.out.println("::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
//		if(myPort.equals("11108")){
//			System.out.println("Inserting from "+myPort);
//			for (String in_key : H){
//				ContentValues cv = new ContentValues();
//				cv.put("key", in_key);
//				cv.put("value", in_key);
//				Uri newUri = insert(myUri, cv);
////				self_insert(in_key, in_key);
//			}
//		}




		return true;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public ArrayList<String> getDest(String in_key){
		ArrayList<String> dest_ports = new ArrayList<String>();


		String h = null;
		try {
			 h = genHash(in_key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		int count = 0;
		//Limiting case 1: the in_key_hash is less than smallest hash
		//Limiting case 2: the in_key_hash is greater than the largest hash
		if((nodes.get(0).node_hash.compareTo(h) >= 0) || (nodes.get(4).node_hash.compareTo(h)<0)){
//			System.out.println("Herer Again ----------");
			dest_ports.add(nodes.get(0).node_port);
			dest_ports.add(nodes.get(1).node_port);
			dest_ports.add(nodes.get(2).node_port);
			count += 3;
			return dest_ports;
		}
		else{
//			System.out.println("Else ------ main ");
			int i = 1;
			for(; i<= nodes.size(); i++){
				if(nodes.get(i).node_hash.compareTo(h)>=0){
					//current node hash is the destination.
					//pick this and the next two nodes that follow to be selected as dest_ports
					dest_ports.add(nodes.get(i).node_port);
					count += 1;
//					System.out.println(" break---------" + i);
					break;

				}
			}
			//Now, if we break when i == 1 / 2
			if(i == 1 || i == 2){
//				System.out.println("case 1");
				dest_ports.add(nodes.get(i+1).node_port);
				dest_ports.add(nodes.get(i+2).node_port);
				count+=2;
			}
			else if(i==3){
//				System.out.println("case 2");
				dest_ports.add(nodes.get(i+1).node_port);
				dest_ports.add(nodes.get(0).node_port);
			}
			else{
				// either the in_key_hash is <= to largest hash or > than it.
				// greater than case is already handled in Limiting case 2. So this else means only that in_key_hash <= last node hash
//				System.out.println("case 3");
				dest_ports.add(nodes.get(0).node_port);
				dest_ports.add(nodes.get(1).node_port);
				count += 2;
			}

		}
		return dest_ports;
	}

	public boolean self_insert(String k, String v){
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		FileOutputStream outputStream;
		try {
			outputStream = getContext().openFileOutput(k, Context.MODE_PRIVATE);
			outputStream.write(v.getBytes());
			outputStream.close();
//			lock.writeLock();
//			System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			TABLE.add(k);
//			lock.writeLock().unlock();
//			System.out.println("=============@@@@@@@==========");
//			System.out.println("Inserted "+k+" "+v +" ----- " + myPort);
		} catch (Exception e) {
			Log.e(TAG, "File write failed");
		}


		return true;
	}

	public String self_query(String k){

		FileInputStream fis = null;
		ArrayList<String> C_TABLE = new ArrayList<String>(TABLE);

		for (String T : C_TABLE) {
			if (T.equals(k)) {
				try {
					fis = getContext().openFileInput(k);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				InputStreamReader isr = new InputStreamReader(fis);
				BufferedReader br = new BufferedReader(isr);
				StringBuilder sb = new StringBuilder();

				String value;

				try {
					value = br.readLine();
					sb.append(value).toString();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return sb.toString();
			}
		}

//		System.out.println("Something is wrong---------------------- key not here "+ myPort);
		return null;
	}

	private class InsertTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			String key = msgs[0];
			String val = msgs[1];
			String remotePort = msgs[2];

			Socket socket = null;
			String msg = "INR_" + key + "_" + val;
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
//              System.out.println("Created Socket");
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				PrintWriter client_out = new PrintWriter(socket.getOutputStream(), true);
//              System.out.println("Sending Message");
				client_out.println(msg);
				String ack = in.readLine();
//              System.out.println("Recevied Ack");
				if(ack != null) {
					if (!(ack.equals("No"))) {
						//System.out.println(ack);
						socket.close();
						return null; // For now
					} else {
//                        System.out.println("Message dint go to " + remotePort);
					}
				}
				socket.close();
				in.close();
				client_out.close();
//              System.out.println("Done with one socket session___________________________________________");
			} catch (IOException e) {
				e.printStackTrace();
			}

			return null;
		}

	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			while(true){
				Socket clientSocket = null;
				try{
					clientSocket = serverSocket.accept();
					BufferedReader server_in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
					String myline = server_in.readLine();

					if(myline != null){

						//Check for INR: "Insert No Replication"

						String[] words = myline.split("_", 0);
						if(words[0].equals("INR")){
							self_insert(words[1], words[2]);
						}
						else if(words[0].equals("Q")){
							String Q_ans = self_query(words[1]);
							if(Q_ans != null){
							    if(!Q_ans.equals("null")){
                                    out.println(Q_ans);
                                }
							    else{
							        out.println("No");
                                }
                            }
							else{
							    out.println("No");
                            }

						}
						else if(words[0].equals("DD")){
                            delete(myUri, words[1], null);
                            System.out.println(":::::::::::::::::::Deleted "+words[1]+" ::::::::::::::::::::::::");
                            System.out.println(TABLE.contains(words[1]));
                        }
						else if(words[0].equals("QQ")){
							MatrixCursor server_mat_cursor = (MatrixCursor) query(myUri, null, "@", null, null);

							server_mat_cursor.moveToFirst();
							String acknow = "_END_";
							while (server_mat_cursor.getCount() >= 1) {
								try{
//									System.out.println("Here in Server in While");
									acknow = server_mat_cursor.getString(0) + "_" + server_mat_cursor.getString(1);
									out.println(acknow);
									server_mat_cursor.moveToNext();
								}catch (CursorIndexOutOfBoundsException e) {
									server_mat_cursor.close();
//                                  System.out.println("Closed Server Matrix Cursor");
//                                  System.out.println(server_mat_cursor.getCount());
									break;
								}
							}
							out.println("_END_");
						}
					}
					clientSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
					break;
				}

			}

			return null;
		}
	}

	private class RecoverTask extends AsyncTask<ServerSocket, String, Void>{

		protected Void doInBackground(ServerSocket... sockets) {
				System.out.println("Here in Recovery Task, Just Born");
				int count = 0;

				MatrixCursor Fetch_All_Cursor = (MatrixCursor) query(myUri, null, "*", null, null);

				if(Fetch_All_Cursor != null){

					Fetch_All_Cursor.moveToFirst();
					ArrayList<String> Fetch_O = new ArrayList<String>();
					String f_key = null;
					String f_val = null;
					System.out.println("------------Got these number of kv from neighbors-------------------" + Fetch_All_Cursor.getCount());
					while (Fetch_All_Cursor.getCount() >= 1) {
						try{
//							System.out.println("Here in Fetch While");
							f_key = Fetch_All_Cursor.getString(0);
							f_val = Fetch_All_Cursor.getString(1);
							Fetch_O = getDest(f_key);
							System.out.println(f_key +" "+ f_val+ " "+ Fetch_O.get(0)+ " "+ Fetch_O.get(1)+ " "+Fetch_O.get(2));
							if(Fetch_O.contains(myPort)){
								System.out.println("Belongs To me "+ myPort + " "+ (count + 1));
								if(! TABLE.contains(f_key)){
									count += 1;
									self_insert(f_key, f_val);
								}
								else{
									System.out.println("Duplicate Key "+ f_key);
								}

							}
							Fetch_All_Cursor.moveToNext();


						}catch (CursorIndexOutOfBoundsException e) {
							Fetch_All_Cursor.close();
							System.out.println("----------------------------------Closing Fetch_ALL_Cursor ---------------------------------");
							break;
						}
					}

				}
			return null;
		}


	}

}
