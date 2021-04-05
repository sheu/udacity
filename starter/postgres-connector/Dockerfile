FROM confluentinc/cp-kafka-connect
USER root
RUN curl -k -SL https://jdbc.postgresql.org/download/postgresql-42.2.19.jar -o postgresql-42.2.19.jar && cp postgresql-42.2.19.jar /usr/share/java/kafka/
RUN ls -lrth /usr/share/java/kafka
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest