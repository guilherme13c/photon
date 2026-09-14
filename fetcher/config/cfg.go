package config

type Cfg struct {
	MaxRoutines           int
	KafkaBroker           string
	KafkaTopic            string
	KafkaProducerTopic    string
	KafkaDynamicUrlsTopic string
	KafkaGroup            string
	KafkaDlqTopic         string
	FrontierURL           string
	PrometheusPort        string
}
