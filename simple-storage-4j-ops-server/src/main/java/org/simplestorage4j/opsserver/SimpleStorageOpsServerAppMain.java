package org.simplestorage4j.opsserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"org.simplestorage4j.opsserver", "org.simplestorage4j.opscommon"})
public class SimpleStorageOpsServerAppMain {

	public static void main(String[] args) {
		SpringApplication.run(SimpleStorageOpsServerAppMain.class, args);
	}

}
