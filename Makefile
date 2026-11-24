JAVA_HOME ?= $(HOME)/.jdks/temurin-21
MVN ?= JAVA_HOME=$(JAVA_HOME) mvn

.PHONY: build test run clean jar

build:
	$(MVN) -B compile

test:
	$(MVN) -B test

run:
	$(MVN) -B -q exec:java

clean:
	$(MVN) -B clean

jar: build
	mkdir -p target
	$(JAVA_HOME)/bin/jar cfe target/broker.jar com.miet.tpp.messagebroker.App -C target/classes .
	$(JAVA_HOME)/bin/jar cfe target/client.jar com.miet.tpp.messagebroker.client.InteractiveClient -C target/classes .
