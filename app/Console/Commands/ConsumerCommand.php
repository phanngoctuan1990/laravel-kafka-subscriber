<?php

namespace App\Console\Commands;

use App\Stat;
use Exception;
use RdKafka\Conf;
use RdKafka\Message;
use RdKafka\KafkaConsumer;
use Illuminate\Console\Command;

class ConsumerCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'kafka:consume';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Kafka consumer';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Get kafka config
     *
     * @return \RdKafka\Conf
     */
    protected function getConfig()
    {
        $conf = new Conf();

        // Configure the group.id. All consumer with the same group.id will consume
        // different partitions.
        $conf->set('group.id', 'myConsumerGroup');

        // Initial list of Kafka brokers
        $conf->set('metadata.broker.list', env('KAFKA_BROKERS', 'kafka:9092'));

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        $conf->set('auto.offset.reset', 'smallest');

        // Automatically and periodically commit offsets in the background
        $conf->set('enable.auto.commit', 'false');

        return $conf;
    }

    /**
     * Decode kafka message
     *
     * @param Message $kafkaMessage kafkaMessage
     *
     * @return object
     */
    protected function decodeKafkaMessage(Message $kafkaMessage)
    {
        $message = json_decode($kafkaMessage->payload);

        if (is_string($message->body)) {
            $message->body = json_decode($message->body);
        }

        return $message;
    }

    /**
     * Process Kafka message
     *
     * @param Message $kafkaMessage kafkaMessage
     *
     * @return void
     */
    protected function processMessage(Message $kafkaMessage)
    {
        $message = $this->decodeKafkaMessage($kafkaMessage);

        $this->info(json_encode($message));

        Stat::updateOrCreate(
            ['inventory_id' => $message->body->id],
            ['make' => $message->body->make, 'model' => $message->body->model]
        );
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $consumer = new KafkaConsumer($this->getConfig());

        // Subscribe to topic 'inventories'
        // Microservice 1 will push to 'inventories' topic
        $consumer->subscribe(['inventories']);

        while (true) {
            $message = $consumer->consume(120 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->processMessage($message);
                    // Commit offsets asynchronously
                    $consumer->commitAsync($message);
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;

                default:
                    throw new Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }
}
