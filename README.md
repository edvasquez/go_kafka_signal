# go_kafka_signal
generate signal for a thing

go build producer.go

./producer -f ~/Develop/Lojack/Magenta/Go/Signal_kafka/examples/clients/cloud/go/kafka.config -t com.magenta.thing.gps.signal


docker build --build-arg CI_PRIVATE_KEY="$(cat ~/.ssh/sshkey)" -t local/agent_service:dev .
docker-compose -f docker-tsdb_writer.yaml up