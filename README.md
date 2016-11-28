# Rrabbit

Rrabbit is a work-in-progress R package that implements a RabbitMQ client. It's
just a wrapper around the [RabbitMQ C AMQP](https://github.com/alanxz/rabbitmq-c)
client library for C. The API hasn't yet reached a stable point.

Our goal wasn't to implement everything, but only a subset of features to suit
the needs of our use case. Bug reports, fixes, implementation of missing features
and other kinds of contributions are most welcome.

## Dependencies

To be able to use *Rrabbit*, you must first install *RabbitMQ C AMQP* as a shared
library. We recommend installing it using the second, alternative method:

```
autoreconf -i
./configure
make
make install
```

For further information, please, refer to https://github.com/alanxz/rabbitmq-c.

## Supported OS

Currently only Linux and Mac are supported.
