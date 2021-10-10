# NewsClassifier
Classifing the news into different categories

Please find the Milestone report and the highlevel architecture documents in the documents/week1 folder.

We have defined a CI-CD workflow that will build the docker images. It can be found under .github/workflows.

As part of Capstone project's Week1 module, we have created a docker pipeline to launch the Zookeeper, Kafka, Spark, MongoDB and the Kafka Producer and Consumer services. We are using the https://rapidapi.com/newscatcher-api-newscatcher-api-default/api/free-news/ to fetch the news articles. Data will be received as a json response from the website. We are processing the data to retrieve the below fields from the JSON object.
title
date/ time
summary
topic/ category
source
We are streaming the data to the consumer using the Kafka broker. From the consumer side we are extracting the data from the Kafka broker and pushing the same into MongoDB.
