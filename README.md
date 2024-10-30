> [!WARNING]
>
> # ⚠️  this project was a PoC that is no longer maintained
>
> Since then:
> - IPFS Ecosystem adopted [Trustless Gateway Specification](https://specs.ipfs.tech/http-gateways/trustless-gateway/) 
>   - Partial DAG transwer was introduced in [IPIP-0402: Partial CAR Support on Trustless Gateways](https://specs.ipfs.tech/ipips/ipip-0402/)
> 
> If you want to run a production-grade HTTP Gateway consider migrating to [Boxo SDK's `boxo/gateway` library](https://github.com/ipfs/boxo/) or [Rainbow daemon](https://github.com/ipfs/rainbow/) which support both of the above and more.

# StarGate

> A Robust Verifiable HTTP Protocol For Content Addressed Data

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Overview

StarGate is a specification to extend the IPFS gateway to support trustless, multipeer data transfer of fairly complex queries. 

This repo is a prototype design and implementation of StarGate, containing a simple executable that implements a Stargate server and client (client still in development as of 1/18/2023).

For more information on how Stargate works and its core design goals see the StarGate talk from the Move The Bytes Working Group: https://www.youtube.com/watch?v=qbKZmmMZePs and the [StarGate protocol specification](https://www.notion.so/pl-strflt/StarGate-be818445cfe44898b2e895d89301d463)

## Setup

To install Stargate, run:

```
> go install github.com/ipfs/stargate/cmd/stargate@latest
```

Then, setup your repo with:

```
> stargate init
```

*Note*: You don't really have to run stargate init for the time being cause the other commands will initialize everything if it's not done.

## Usage

### Import data

Import the current directory, recursively:

```
> stargate --vv import .
Sending CID bafybeidwarsw46q7wx5jrojwzgg4smvmgvgj23chzmybidten3l7wjnrva through the Stargate!
```

Import a file:
```
> stargate --vv import ~/Downloads/testvideo.mp4 
Sending CID bafybeigkkzgkd6z33jaczjhrmjb5m3jwqyn7zbbfvmy2ekfm6dievp5kdy through the Stargate!
```

### Run the Stargate Server

```
> stargate --vv server
```

(the server can start any time and you can import while the server is running)

### Fetch (with CURL for now)

Fetch the root directory:
```
> curl -v http://localhost:7777/ipfs/bafybeidwarsw46q7wx5jrojwzgg4smvmgvgj23chzmybidten3l7wjnrva > root.car
```

Pathing:
```
> curl -v http://localhost:7777/ipfs/bafybeidwarsw46q7wx5jrojwzgg4smvmgvgj23chzmybidten3l7wjnrva/go.mod > go.mod.car

> curl -v http://localhost:7777/ipfs/bafybeidwarsw46q7wx5jrojwzgg4smvmgvgj23chzmybidten3l7wjnrva/pky/types.go > types.go.car
```

Fetch a file:
```
> curl -v http://localhost:7777/ipfs/bafybeigkkzgkd6z33jaczjhrmjb5m3jwqyn7zbbfvmy2ekfm6dievp5kdy > testvideo.mp4.car
```

Fetch a file, but don't send leaf blocks (useful for multipeer):

```
> curl -v http://localhost:7777/ipfs/bafybeigkkzgkd6z33jaczjhrmjb5m3jwqyn7zbbfvmy2ekfm6dievp5kdy?noleaves  > testvideo.mp4.dag.car
```

Fetch a range (of the flat file, not the car):

```
> curl -v http://localhost:7777/ipfs/bafybeigkkzgkd6z33jaczjhrmjb5m3jwqyn7zbbfvmy2ekfm6dievp5kdy?bytes=0-1000000  > testvideo.mp4.start.car
```

## Documentation

See [Go Doc](https://pkg.go.dev/github.com/ipfs/stargate)

## Roadmap

A list of things to do:

- Complete Fetch Command
- Add Tracing and Metrics
- MOAR documentation
- Measure Performance
- Optimizations
- Filecoin Chain Resolver

But sadly, little time for original author to complete them

## Contribute

Early days PRs are welcome!

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2022. Protocol Labs, Inc.
