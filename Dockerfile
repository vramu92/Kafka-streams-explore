FROM amazoncorretto:17-alpine-jdk
LABEL authors="ramisstudio"
RUN mkdir -pv \
    /kafka-streams-explore/kafka-stream/state-stores/ \
    /kafka-streams-explore/kafka-stream/state-stores/logs

RUN apk update
RUN addgroup -g 1001 app_user && adduser -D -u 1001 -h /kafka-streams-explore app_user -G app_user

#Run as app_user
USER app_user
WORKDIR /kafka-streams-explore

COPY target/kafka-streams-explore.jar /kafka-streams-explore