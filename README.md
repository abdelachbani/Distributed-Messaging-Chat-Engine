# Distributed Messaging & Multi-Channel Chat Engine: Dual-Architecture RPC (Java RMI) & Enterprise Message Broker (JMS / Apache Artemis)

## Overview

This repository features a distributed, real-time, multi-channel messaging platform implemented across two distinct distributed computing paradigms:
1. **Remote Procedure Calls (RPC)** via **Java Remote Method Invocation (RMI)**.
2. **Message-Oriented Middleware (MOM)** via **Java Message Service (JMS)** backed by an **Apache ActiveMQ Artemis** broker.

Both architectures implement identical business contracts: dynamic user authentication, multi-room channel broadcasting, private peer-to-peer messaging, automated presence detection (join/leave events), and an asynchronous graphical interface built with Swing.

---

## Architectural Comparison

| Dimension | Java RMI Implementation (`rmi/`) | JMS / Apache Artemis Implementation (`jms/`) |
| :--- | :--- | :--- |
| **Paradigm** | Distributed Object Model (RPC / Remote Interface Stubs) | Message-Oriented Middleware (Asynchronous Message Passing) |
| **Coordination** | Standalone registry / NameServer (`INameServer`) | Enterprise Message Broker (JNDI Context & Broker Queues) |
| **Communication** | Direct TCP sockets via RMI Stubs/Skeletons | Decoupled client-to-broker connections |
| **Broadcasting** | Server-side iteration delivering remote callback invocations | Native Publish/Subscribe Topics (`dynamicTopics/channel-*`) |
| **Private Messaging** | Direct remote method invocation on client stub (`IChatUser`) | Point-to-Point queues (`dynamicQueues/user-*`) |
| **Coupling** | High temporal and spatial coupling (both endpoints active) | Loose temporal and spatial coupling (broker queues) |

---

## System Architecture

### 1. Java RMI Architecture

```
                    ┌─────────────────────────┐
                    │       NameServer        │
                    │ (Service Registration & │
                    │       Discovery)        │
                    └────────────┬────────────┘
                                 │ Lookup
            Bind / Export        │
      ┌──────────────────────────┴─────────────────────────┐
      │                                                    │
┌─────▼──────────┐         Remote Callbacks          ┌─────▼──────────┐
│   ChatServer   │ ◄───────────────────────────────► │   ChatClient   │
│  (Channels &   │                                   │ (Swing UI &    │
│  State Logic)  │ ◄───────────────────────────────► │  User Stub)    │
└────────────────┘         Remote Invocations        └────────────────┘
```

- **Service Registry (`NameServer`)**: Central directory binding and discovering distributed objects (`IChatServer`, `INameServer`).
- **Remote Callbacks (`IChatUser`, `MessageListener`)**: Clients export remote stubs. When a message is sent to a channel, the server asynchronously dispatches invocations to each connected client's stub.
- **Concurrency Control**: State operations (channel joins, message fan-out, user listings) use thread-safe synchronizers (`SynchroMap`).

### 2. JMS / Apache Artemis Architecture

```
                                  ┌───────────────────────────┐
                                  │   Apache Artemis Broker   │
                                  │                           │
                                  │ ┌───────────────────────┐ │
                                  │ │  dynamicQueues/       │ │
                                  │ │  ChatServer           │ │
                                  │ └───────────▲───────────┘ │
                                  │             │             │
                    ┌─────────────┼─────────────┼─────────────┼─────────────┐
                    │             │             │             │             │
                    ▼             ▼             │             ▼             ▼
       ┌───────────────────────┐ ┌──────────────┴──────────┐ ┌───────────────────────┐
       │     dynamicTopics/    │ │      dynamicQueues/     │ │     dynamicTopics/    │
       │    channel-<name>     │ │       user-<nick>       │ │    channel-<name>     │
       │     (Channel Pub)     │ │   (Direct Messages)     │ │     (Channel Sub)     │
       └────────────▲──────────┘ └──────────────┬──────────┘ └────────────┬──────────┘
                    │                           │                         │
                    │                           │                         │
             ┌──────┴──────┐             ┌──────▼──────┐           ┌──────▼──────┐
             │ Client A    │             │ Client B    │           │ Client C    │
             └─────────────┘             └─────────────┘           └─────────────┘
```

- **Server Command Queue (`dynamicQueues/ChatServer`)**: Point-to-Point request/reply queue handling initial connections, channel discovery, and join operations using temporary reply queues (`JMSReplyTo`).
- **Channel Topics (`dynamicTopics/channel-<name>`)**: Pub/Sub destinations for multi-tenant channel broadcasting.
- **Private Queues (`dynamicQueues/user-<nick>`)**: Dedicated queues delivering private direct messages directly to specific user instances.
- **Typed Message Hierarchy**: Strongly-typed payload protocols (`ConnectMessage`, `JoinMessage`, `ChatMessage`, `UserJoinsMessage`, `UserLeavesMessage`) wrapped via `MessageFactory`.

---

## Directory Layout

```text
Distributed-Messaging-And-Chat-Engine/
├── rmi/
│   ├── src/
│   │   ├── ChatClient.java         # RMI client bootstrap & GUI wiring
│   │   ├── ChatRobot.java          # Automated headless stress-test bot
│   │   ├── ChatServer.java         # RMI server bootstrap & channel registrar
│   │   ├── NameServer.java         # Service directory daemon
│   │   ├── faces/                  # RMI remote interfaces
│   │   │   ├── IChatChannel.java
│   │   │   ├── IChatMessage.java
│   │   │   ├── IChatServer.java
│   │   │   ├── IChatUser.java
│   │   │   ├── INameServer.java
│   │   │   └── MessageListener.java
│   │   ├── impl/                   # Remote object implementations
│   │   │   ├── ChatChannelImpl.java
│   │   │   ├── ChatMessageImpl.java
│   │   │   ├── ChatServerImpl.java
│   │   │   ├── ChatUserImpl.java
│   │   │   └── NameServerImpl.java
│   │   ├── ui/                     # Swing UI model & renderer
│   │   ├── utils/                  # Synchronized map & network utilities
│   │   └── utils_rmi/              # RMI socket & configuration helper
│   └── lib/
│       └── flatlaf-1.2.jar
├── jms/
│   ├── src/
│   │   ├── ChatClientJMS.java      # JMS client listener & publisher
│   │   ├── ChatRobot.java          # Automated client test bot
│   │   ├── ChatServerJMS.java      # Server queue consumer & channel router
│   │   ├── messages/               # Serializable JMS payload protocol
│   │   │   ├── AMessage.java
│   │   │   ├── ChatMessage.java
│   │   │   ├── ConnectMessage.java
│   │   │   ├── ConnectOkMessage.java
│   │   │   ├── JoinMessage.java
│   │   │   ├── JoinOkMessage.java
│   │   │   ├── MessageFactory.java
│   │   │   ├── UserJoinsMessage.java
│   │   │   └── UserLeavesMessage.java
│   │   ├── ui/                     # Shared Swing UI presentation layer
│   │   ├── utils/                  # Thread-safe synchronization utilities
│   │   └── utils_jms/              # Artemis JNDI context & destination bindings
│   ├── config/
│   │   └── jndi.properties         # JNDI initial context connection settings
│   ├── scripts/
│   │   └── setup-artemis.sh        # Broker creation script
│   └── lib/
│       ├── artemis-jms-client-all-2.17.0.jar
│       └── flatlaf-1.2.jar
├── .gitignore
└── README.md
```

---

## Compilation & Execution

### Prerequisites
- Java Development Kit (JDK) 11 or newer.
- Local Apache ActiveMQ Artemis broker (for the JMS module).

---

### 1. Compiling the Engines

#### Bash
```bash
# Compile RMI Engine
mkdir -p rmi/bin
javac -cp "rmi/lib/*" -d rmi/bin $(find rmi/src -name "*.java")

# Compile JMS Engine
mkdir -p jms/bin
javac -cp "jms/lib/*" -d jms/bin $(find jms/src -name "*.java")
```

#### PowerShell
```powershell
# Compile RMI Engine
New-Item -ItemType Directory -Force rmi/bin
$rmiSources = (Get-ChildItem -Recurse -Filter *.java rmi/src).FullName
javac -cp "rmi/lib/flatlaf-1.2.jar" -d rmi/bin $rmiSources

# Compile JMS Engine
New-Item -ItemType Directory -Force jms/bin
$jmsSources = (Get-ChildItem -Recurse -Filter *.java jms/src).FullName
$jmsLibs = "jms/lib/artemis-jms-client-all-2.17.0.jar;jms/lib/flatlaf-1.2.jar"
javac -cp $jmsLibs -d jms/bin $jmsSources
```

---

### 2. Running Option A: Java RMI Architecture

Run each component in a separate terminal:

#### 1. Start the Name Server (Service Registry)
```bash
java -cp rmi/bin NameServer
```

#### 2. Start the Chat Server
```bash
java -cp rmi/bin ChatServer
```

#### 3. Launch One or More Chat Clients
```powershell
java -cp "rmi/bin;rmi/lib/flatlaf-1.2.jar" ChatClient
```

#### 4. (Optional) Run Automated Test Robot
```bash
java -cp rmi/bin ChatRobot
```

---

### 3. Running Option B: JMS / Apache Artemis Architecture

#### 1. Start Local Artemis Broker
Ensure an Apache Artemis broker instance is active on `tcp://localhost:61616`.

#### 2. Start the JMS Chat Server
```powershell
$jmsCp = "jms/bin;jms/lib/artemis-jms-client-all-2.17.0.jar;jms/lib/flatlaf-1.2.jar"
java -cp $jmsCp ChatServerJMS
```

#### 3. Launch JMS Client Instances
```powershell
$jmsCp = "jms/bin;jms/lib/artemis-jms-client-all-2.17.0.jar;jms/lib/flatlaf-1.2.jar"
java -cp $jmsCp ChatClientJMS
```
