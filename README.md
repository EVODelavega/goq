# goq
Abstraction layer for queues

## How to use

For now, only the consuming aspect is implemented, and only AWS is supported. There is an example package, where the message type is being wrapped into a more specific data-type. The `New` function for these message types will be the only parts of the users' code where AWS specific code is found. After that, all uses of the messages and queue interaction should go through the provided interfaces.
