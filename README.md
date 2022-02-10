# Peerster

Homework material for the Decentralized Systems Engineering course.

Peerster is a gossip-based P2P system. 

Architecture:

```
┌──────┐  ┌───────────┬───────────┬───────────────────┐
│      │  │           │           │                   │
│      │  │           │           │ P2P Data naming   │
│      │  │           │           │                   │
│      │  │ P2P text  │ P2P data  ├───────────────────┤
│      │  │ messaging │ sharing   │                   │
│      │  │           │           │ TLC + Blockchain  │
│ Sto- │  │           │           │                   │
│ rage │  │           │           ├───────────────────┤
│      │  │           │           │                   │
│      │  │           │           │ Paxos             │
│      │  │           │           │                   │
│      │  ├───────────┴───────────┴───────────────────┤
│      │  │                                           │
│      │  │ Gossip protocol                           │
│      │  │                                           │
└──────┘  ├───────────────────────────────────────────┤
          │                                           │
          │ Network                                   │
          │                                           │
          └───────────────────────────────────────────┘
```

See [design document](docs/README.md) for more details.
