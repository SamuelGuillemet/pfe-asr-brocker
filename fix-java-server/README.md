Requirements:
-have kafka broker running on localhost:9092
Run:
-Start the server with: mvn exec:java -Dexec.mainClass="fix.server.Executor"
-Start the relay with: mvn exec:java -Dexec.mainClass="fix.relay.KafkaRelay"
