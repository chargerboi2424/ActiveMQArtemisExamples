# ActiveMQArtemisExamples
Examples of consumers/producers demonstrating how to send different things through AMQ Artemis

It is written in two pieces: the consumer that does all the work (aka the real program) and the test client that interacts with it (aka replace with real code.)

The examples include:
- Sending a text message
- Sending a file
- Sending a json object
- Sending a request for both get/post rest calls and getting the results back
- Having a shutdown/restart mechanism

These were all written with a particular use case in mind, but hopefully they'll be useful to someone.
