package config

type Cfg struct {
	MaxRoutines        int
	KafkaBroker        string
	KafkaTopic         string
	KafkaProducerTopic string
	KafkaGroup         string
}
