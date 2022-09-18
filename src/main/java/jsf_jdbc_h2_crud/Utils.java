package jsf_jdbc_h2_crud;

public class Utils {
	
	public static void close(AutoCloseable... autoCloseables) throws Exception {		
		for (AutoCloseable autoCloseable : autoCloseables)
			if (autoCloseable != null)
				autoCloseable.close();
	}

}
