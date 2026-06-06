# RabbitMQ stream tester

This is a small tester for consuming RabbitMQ streams.

It's a cargo crate with three programs:
1. producer
2. consumer
3. push_mock

## Producer

Just connects to RabbitMQ stream, writes 100_000 messages like:

> Hello RabbitMQ! Message no {i}

It's only purpose is to fill the stream with test data.

## Consumer

Connects to the stream and starts reading messages.
For each message makes a http call to `http://localhost:7878/push` with the body set to the read message.

As this is a test for streams the messages will not be consumed, they remain in the stream.  
If after a restart or crash the consumer needs to resume from the last message written it will need offset persistance.

RabbitMQ supports a server-based offset tracking but recommends to be called every each 10_000 reads or so.
This is not granular enough if the client needs to resume from the last read message.

As such, the consumer will need to maintain and **persist** the current offset.

This consumer will save it's current offset in the stream in a local `sled` db stored in `/tmp/rabbit_consumer_storage`.
At startup it will try to read the saved offset from the local db and start from it.
If not found it will start from the first message in the stream.

PS: I also ran the test with offset saving disabled, just to see the speed impact.
Test results are summarized at the end.

## Push mockup

The `push_mock` is just a http server listening on :7878.
For each `post` on `/push` it will output an info log message.


### Config
Config variables are directly in src/config.rs, these are default values:

    rabbitmq_host: String::from("127.0.0.1"),
    rabbitmq_port: 5552,
    rabbitmq_user: String::from("guest"),
    rabbitmq_password: String::from("guest"),
    rabbitmq_stream: String::from("stream01"),
    push_url: String::from("http://localhost:7878/push"),
    local_storage: "/tmp/rabbit_consumer_storage".to_string(),


### Test results

Speed test have been peformed in three configurations:

1. No stream offset is persisted by the consumer
 > Consumer process speed: *30_000* msg/s

2. The consumer persists the offset in a synchronous manner, using a sync flush after each message.
 > Consumer process speed: ~ *250* msg/s.

3. The consumer persists the offset using a separate async worker with a buffer of 10.  
Of course in case of a consumer crash it's offset counter could be 10 message behind.
 > Consumer process speed: ~ *250* msg/s.

In the last case, even if the flush is done on a separate worker, it has a limited buffer of 10.
This fills up very quickly and the channel stops accepting new flush requests.
Which will in fact **syncronize** the message processing with the persistence worker.

Increasing the buffer is *not* a solution, as this will leave the persisted offset behind the actual reads.

**Conclusion:** for maximum speed do not persist the read offset, find other method to identify the last message.
Or just embrace the speed penalty and go full sync with the offset persistence.

