# P2PRadio

A Radio built on top of libp2p GossipSub.

The idea is to take advantage of the capabilities of the GossipSub protocol to allow broadcasting radio to scale efficiently,
instead of having an n-to-n communication model.

Listeners can tune in by choosing the topic they want to listen to and making sure the public key is correct.
Every listener is also a peer on the GossipSub network and they help distribute the messages to other peers.