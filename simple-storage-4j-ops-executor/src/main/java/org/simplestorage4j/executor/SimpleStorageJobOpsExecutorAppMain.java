package org.simplestorage4j.executor;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

//import org.springframework.boot.SpringApplication;

@Slf4j
public class SimpleStorageJobOpsExecutorAppMain {

	public static void main(String[] args) {
		val app = new SimpleStorageJobOpsExecutorAppMain();
		try {
			app.parseArgs(args);
			app.run();
		} catch(Throwable ex) {
			log.error("Failed, exiting (-1)", ex);
			System.out.println("(stdout) Failed, exiting (-1)");
			ex.printStackTrace(System.out);
			System.err.println("(stderr) Failed, exiting (-1), ex:" + ex.getMessage());
			System.exit(-1);
		}
	}

	private void parseArgs(final String[] args) {
		for(int i = 0; i < args.length; i++) {
			
		}
		// TODO Auto-generated method stub
	}


	private void run() {
		
		// TODO Auto-generated method stub
		
	}

}
