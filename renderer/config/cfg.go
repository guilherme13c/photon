package config

type Cfg struct {
	MaxRoutines        int
	BrowserConcurrency int
	KafkaBroker        string
	KafkaTopic         string
	KafkaProducerTopic string
	KafkaGroup         string
	FrontierURL        string
}
