package org.simplestorage4j.executor;

import org.simplestorage4j.api.BlobStorageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = {"org.simplestorage4j.executor", "org.simplestorage4j.opscommon"})
public class SimpleStorageJobOpsExecutorAppMain {

	public static void main(String[] args) {
		try {
			SpringApplication.run(SimpleStorageJobOpsExecutorAppMain.class, args);
		} catch(Throwable ex) {
			log.error("Failed, exiting (-1)", ex);
			System.out.println("(stdout) Failed, exiting (-1)");
			ex.printStackTrace(System.out);
			System.err.println("(stderr) Failed, exiting (-1), ex:" + ex.getMessage());
			System.exit(-1);
		}
	}

}

@Component
@Slf4j
class AppCmdLineRunner implements CommandLineRunner {

	@Autowired 
	protected BlobStorageRepository storageRepo;
	
	@Override
	public void run(String... args) throws Exception {
		log.info("starting..");
		val repos = storageRepo.findAll();
		log.info("repos: " + repos);
		
	}
	
}
