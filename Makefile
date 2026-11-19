JAVA_HOME ?= $(HOME)/.jdks/temurin-21
MVN ?= JAVA_HOME=$(JAVA_HOME) mvn

.PHONY: build test run clean

build:
	$(MVN) -B compile

test:
	$(MVN) -B test

run:
	$(MVN) -B -q exec:java

clean:
	$(MVN) -B clean
