Email Analysis
==============

## Overview
Analyze the enron dataset and find interesting statistics and patterns. Some of the things answers by this project are:
- How many emails did each person receive each day?
- Let's label an email as "direct" if there is exactly one recipient and "broadcast" if it has multiple recipients. Identify the person (or people) who received the largest number of direct emails and the person (or people) who sent the largest number of broadcast emails.
- Find the five emails with the fastest response times. (A response is defined as a message from one of the recipients to the original sender whose subject line contains all of the words from the subject of the original email, and the response time should be measured as the difference between when the original email was sent and when the response was sent.)

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


