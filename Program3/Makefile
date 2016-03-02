program = activities
inputFilepath = ./test/
inputFile = bfTest100.json
outputFilepath = ./test/l7p3output
jarFilepath = ./../jars/*
mapReduceJarFilepath = ./../jars/json-mapreduce-1.0.jar
jsonJarFilepath = ./../jars/org.json-20120521.jar

default:
	javac -cp "$(jarFilepath)" *.java
jar:
	jar cvf $(program).jar *.class
job:
	hadoop jar $(program).jar $(program) -libjars $(mapReduceJarFilepath),$(jsonJarFilepath) $(inputFilepath)$(inputFile) $(outputFilepath)
remove:
	hadoop fs -rm -r $(outputFilepath)