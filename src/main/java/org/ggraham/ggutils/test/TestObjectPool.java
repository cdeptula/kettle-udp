package org.ggraham.ggutils.test;

import org.ggraham.ggutils.PackageService;
import org.ggraham.ggutils.logging.LogLevel;
import org.ggraham.ggutils.objectpool.ObjectPool;
import org.ggraham.ggutils.objectpool.ObjectPool.PoolItem;

import java.util.ArrayList;

public class TestObjectPool {

	public static void main(String[] args) {

		ObjectPool<StringBuilder> pool = new ObjectPool<StringBuilder>(50, 100, 
				() -> { return new StringBuilder(); });
		
		PackageService.getLog().setLogLevel(LogLevel.DEBUG);
		ArrayList<PoolItem<StringBuilder>> list = new ArrayList<PoolItem<StringBuilder>>();
		for ( int i=0; i < 110; i++ ) {
			list.add(pool.get(false));
		}
		for ( int i=0; i < 110; i++ ) {
			PoolItem<StringBuilder> item = list.remove(0);
			if ( item == null ) continue;
			item.putBack();
		}
				
	}

}
