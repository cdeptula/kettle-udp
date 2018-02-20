package org.ggraham.ggutils.test;

import java.io.Console;

import org.ggraham.ggutils.message.PacketFieldConfig;

public class TestPacketFieldConfigParse {

	public static void main(String[] args) {
		
		PacketFieldConfig[] configs = new PacketFieldConfig[] {
				PacketFieldConfig.getDouble(),
				PacketFieldConfig.getInteger(),
				PacketFieldConfig.getFloat(),
				PacketFieldConfig.getLong(),
				PacketFieldConfig.getVarBinary(),
				PacketFieldConfig.getRemainingBinary(),
				PacketFieldConfig.getFixedBinary(55),
				PacketFieldConfig.getVarString(),
				PacketFieldConfig.getRemainingString(),
				PacketFieldConfig.getFixedString(77),
				PacketFieldConfig.getVarString("Millie"),
				PacketFieldConfig.getRemainingString("Bobby"),
				PacketFieldConfig.getFixedString(88, "Brown")
		};
		
		for ( PacketFieldConfig p : configs ) {
			String dup = p.toString();
			PacketFieldConfig p2 = PacketFieldConfig.fromString(dup);
			System.out.println(p.toString() + ", " + p2.toString());
		}
		System.out.println("STRING(R), " + PacketFieldConfig.fromString("STRING(R)").toString());
		System.out.println("STRING(V), " + PacketFieldConfig.fromString("STRING(V)").toString());
		System.out.println("STRING(777), " + PacketFieldConfig.fromString("STRING(777)").toString());
		
		

	}

}
