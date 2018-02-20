package org.ggraham.ggutils.test;

/*
 * 
 * Apache License 2.0 
 * 
 * Copyright (c) [2017] [Gregory Graham]
 * 
 * See LICENSE.txt for details.
 * 
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Random;

import org.ggraham.ggutils.PackageService;
import org.ggraham.ggutils.logging.LogLevel;
import org.ggraham.ggutils.message.IHandleMessage;
import org.ggraham.ggutils.message.PacketDecoder;
import org.ggraham.ggutils.message.PacketFieldConfig;
import org.ggraham.ggutils.message.FieldType;
import org.ggraham.ggutils.network.UDPSender;
import org.ggraham.ggutils.objectpool.ObjectPool.PoolItem;

public class TestUDPSender {

	// Methods to generate random fields and add to a String Builder
	private static void randomString(Random r, StringBuilder builder, int minlen, int maxlen) {
		int len = minlen + (maxlen<=0 ? 0 : r.nextInt(maxlen));
		for (int i = 0; i < len; i++) {
			builder.append((char) ((int) ('A') + r.nextInt(26)));
		}
	}

	private static void randomInt(Random r, StringBuilder builder, int max) {
		builder.append(r.nextInt(max));
	}

	private static void randomInt(Random r, StringBuilder builder) {
		builder.append(r.nextInt());
	}

	private static void randomLong(Random r, StringBuilder builder) {
		builder.append(r.nextLong());
	}

	private static void randomFloat(Random r, StringBuilder builder) {
		builder.append(r.nextFloat());
	}

	private static void randomDouble(Random r, StringBuilder builder) {
		builder.append(r.nextDouble());
	}

	// Creates a csv file with test data in it.
	private static void createTestData(String filename, int nRows, boolean seq,
			PacketFieldConfig[] fields) throws IOException {
		FileWriter file = new FileWriter(filename);
		BufferedWriter writer = new BufferedWriter(file);
		Random r = new Random();

		try {
			StringBuilder hbuilder = new StringBuilder();
			for (int j = 0; j < fields.length; j++) {
				hbuilder.append(fields[j].toString()).append(",");
			}
			hbuilder.delete(hbuilder.length() - 1, hbuilder.length());
			writer.write(hbuilder.toString());
			writer.newLine();
			for (int i = 0; i < nRows; i++) {
				StringBuilder builder = new StringBuilder();
				for (int j = 0; j < fields.length; j++) {
					switch (fields[j].getFieldType()) {
					case STRING:
					case BINARY:
						if ( fields[j].isFixedLength() ) {
    						randomString(r, builder, fields[j].getFixedLength(), 0);
						} else {
    						randomString(r, builder, 10, 10);
						}
						builder.append(",");						
						break;
					case INTEGER:
						if (seq && j == 0) {
							builder.append(i);
						} else {
							randomInt(r, builder);
						}
						builder.append(",");
						break;
					case LONG:
						randomLong(r, builder);
						builder.append(",");
						break;
					case FLOAT:
						randomFloat(r, builder);
						builder.append(",");
						break;
					case DOUBLE:
						randomDouble(r, builder);
						builder.append(",");
						break;
					default:
						break;
					}
				}
				builder.delete(builder.length() - 1, builder.length());
				writer.write(builder.toString());
				writer.newLine();
			}
		} catch (IOException e) {
			throw (e);
		} finally {
			writer.close();
			file.close();
		}

	}

	public static void usage() {
		System.out.println("Generates and sends random packet data");
		System.out.println("");
		System.out.println("-b N  (optional) bunch size; send N messgaes per packet. (Default: 1)");
		System.out.println("-c N  (optional) generate N packets and store in file given in -f");
		System.out.println("-d d  (optional) delay between packets in milliseconds");
		System.out.println("-f f  (required) reads packets from this csv file, one packet per row");
		System.out.println("-i a  (optional) ip address, default localhost");
		System.out.println("-r p  (optional) specifies port number, default 5555");
		System.out.println("-s    (optional if -c, ignored if ! -c) prepends an integer sequence field to the packet");
		System.out.println("-t t  (required if -c, ignored if ! -c) adds a field of type t to the packet");
		System.out.println("-u    (optional) print usage");
		System.out.println("");
		System.out.println("Examples");
		System.out.println("");
		System.out.println("-f test.csv -c 50000 -t STRING -t INTEGER -t STRING -s");
		System.out.println("Generates 50000 packets with sequence number, random string, integer, and string fields");
		System.out.println("saves in test.csv, and sends on localhost:5555");
		System.out.println("");
		System.out.println("-f test.csv -i 192.168.1.2 -p 6666");
		System.out.println("Reads packets from test.csv, prepends a sequence number and sends to 192.168.1.2:6666");
		System.out.println("");
	}

	public static void main(String[] args) throws IOException, InterruptedException {

		PackageService.getLog().setLogLevel(LogLevel.BASIC);
		ArrayList<Object[]> values = new ArrayList<Object[]>();
		PacketDecoder decoder = new PacketDecoder();

		// Options
		ArrayList<PacketFieldConfig> fields = new ArrayList<PacketFieldConfig>();
		boolean seq = false;
		String filename = "";
		int bunchSize = 1;
		int numGen = 0;
		int port = 5555;
		String address = "localhost";
		int delay = 1;

		int i = 0;
		try {
			while (i < args.length) {
				String currentArg = args[i++];
				if (currentArg.equals("-f")) {
					filename = args[i++];
				} else if (currentArg.equals("-b")) {
					bunchSize = Integer.parseInt(args[i++]);
				} else if (currentArg.equals("-c")) {
					numGen = Integer.parseInt(args[i++]);
				} else if (currentArg.equals("-d")) {
					delay = Integer.parseInt(args[i++]);
				} else if (currentArg.equals("-t")) {
					fields.add(PacketFieldConfig.fromString(args[i++]));
				} else if (currentArg.equals("-s")) {
					seq = true;
				} else if (currentArg.equals("-u")) {
					usage();
				} else if (currentArg.equals("-r")) {
					port = Integer.parseInt(args[i++]);
				} else if (currentArg.equals("-i")) {
					address = args[i++];
				}
			}
		} catch (Exception e) {
			usage();
	    	System.exit(0);
		}

		if (seq) {
			fields.add(0, PacketFieldConfig.getInteger());
		}

		if (filename.isEmpty()) {
			System.out.println("No filename given");
			System.exit(1);
		}
		if (numGen > 0) {
			if (fields.size() == 0) {
				System.out.println("No fields given");
				System.exit(1);
			}
			createTestData(filename, numGen, seq, fields.toArray(new PacketFieldConfig[] {}));
		}

		FileReader file = new FileReader(filename);
		BufferedReader reader = new BufferedReader(file);
		fields.clear();

		try {
			String header = reader.readLine();
			String[] hParts = header.split(",");
			for (int k = 0; k < hParts.length; k++) {
				PacketFieldConfig ft = PacketFieldConfig.fromString(hParts[k].trim());
				decoder.addField(ft);
				fields.add(ft);
			}
			String vline;
			while ((vline = reader.readLine()) != null) {
				String[] vParts = vline.split(",");
				Object[] vals = new Object[vParts.length];
				for (int k = 0; k < fields.size(); k++) {
					switch (fields.get(k).getFieldType()) {
					case INTEGER:
						vals[k] = Integer.parseInt(vParts[k]);
						break;
					case LONG:
						vals[k] = Long.parseLong(vParts[k]);
						break;
					case FLOAT:
						vals[k] = Float.parseFloat(vParts[k]);
						break;
					case DOUBLE:
						vals[k] = Double.parseDouble(vParts[k]);
						break;
					case BINARY:
						vals[k] = vParts[k].getBytes(Charset.forName("US-ASCII"));
						break;
					default:
						vals[k] = vParts[k];
					}
				}
				values.add(vals);
			}
		} catch (IOException e) {
			throw (e);
		} finally {
			reader.close();
			file.close();
		}

		UDPSender sender = new UDPSender(address, port);
		int counter = 0;
		
		while ( counter < values.size() ) {
			PoolItem<ByteBuffer> item = sender.getByteBuffer();
			
			for ( int k = 0; k < bunchSize; k++ ) {
				decoder.EncodePacket(values.get(counter + k), item.getPoolItem());				
				if ( counter + k > values.size() ) break;
			}
			
			sender.send(item);
			if ( delay > 0 ) Thread.currentThread().sleep(delay);
			counter+=bunchSize;
			
		}
		
		System.out.println("Sent " + counter);

	}

}
