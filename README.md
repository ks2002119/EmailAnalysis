email_processing
==============

## Requirements
- Java 8, Maven 3.x
- Download the data files from http://bailando.sims.berkeley.edu/enron/enron_with_categories.tar
.gz and extract it 

## Running unit tests
- `mvn test`: Runs the unit tests relevant to this project
  

## Build & Packaging 
- `mvn package`: Packages the artifact that can be run.

## Configuration
conf directory holds the application.conf file. This file can be used to change analyzer 
configuration to add new email document keywords and skip list words. 


## Steps to run the application

To build the artifact, run `mvn package -DskipTests` command.
To launch analyzer, run `java -jar target/analyzer-jar-with-dependencies.jar`
Note: Launch the java application from the root directory of the project if you are not going to 
pass any command line args. If you are launching the application from any other location, please 
pass the appropriate data directory that contains all the email documents.


