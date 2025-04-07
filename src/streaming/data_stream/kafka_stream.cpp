#include "streaming/data_stream/kafka_stream.h"
#include <iostream>
#include <thread>
#include <librdkafka/rdkafkacpp.h>
namespace candy {

KafkaStream::KafkaStream(std::string name, std::string broker, std::string topic)
    : DataStream(std::move(name), DataFlowType::Kafka),
      broker_(std::move(broker)),
      topic_(std::move(topic)),
      running_(true) {}

KafkaStream::~KafkaStream() {
  running_ = false;
  if (consumer_) {
    consumer_->close();
  }
}

auto KafkaStream::Init() -> void {
  RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  if (!conf) {
    std::cerr << "Failed to create Kafka global configuration" << std::endl;
    return;
  }

  if (conf->set("bootstrap.servers", broker_, errstr_) != RdKafka::Conf::CONF_OK) {
    std::cerr << "Failed to set Kafka broker: " << errstr_ << std::endl;
    delete conf;
    return;
  }

  // Add group.id configuration
  if (conf->set("group.id", "default_group", errstr_) != RdKafka::Conf::CONF_OK) {
    std::cerr << "Failed to set Kafka group.id: " << errstr_ << std::endl;
    delete conf;
    return;
  }

  consumer_.reset(RdKafka::KafkaConsumer::create(conf, errstr_));
  delete conf;

  if (!consumer_) {
    std::cerr << "Failed to create Kafka consumer: " << errstr_ << std::endl;
    return;
  }

  if (consumer_->subscribe({topic_}) != RdKafka::ERR_NO_ERROR) {
    std::cerr << "Failed to subscribe to topic: " << topic_ << std::endl;
    return;
  }

  // Log successful initialization
  std::cerr << "KafkaStream successfully initialized for broker: " << broker_ 
            << ", topic: " << topic_ << std::endl;

  std::thread([this]() {
    while (running_) {
      auto msg = consumer_->consume(1000);
      if (!msg) continue;

      if (msg->err() == RdKafka::ERR_NO_ERROR) {
        auto record = std::make_unique<VectorRecord>();
        // Deserialize msg->payload() into record
        {
          std::lock_guard<std::mutex> lock(mtx_);
          records_.push(std::move(record));
        }
      } else if (msg->err() != RdKafka::ERR__TIMED_OUT) {
        std::cerr << "Kafka error: " << msg->errstr() << std::endl;
      }
      delete msg;
    }
  }).detach();
}

auto KafkaStream::Next(RecordOrWatermark& record_or_watermark) -> bool {
  std::lock_guard<std::mutex> lock(mtx_);
  if (records_.empty()) {
    return false;
  }
  record_or_watermark = std::move(records_.front());
  records_.pop();
  return true;
}

}  // namespace candy
