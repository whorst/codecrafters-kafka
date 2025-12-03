# **Hexagonal Architecture (Ports and Adapters)**

Hexagonal Architecture is a structural pattern invented by Alistair Cockburn. Its primary goal is to completely decouple the core business logic of an application from the external agents, such as databases, user interfaces, third-party services, and testing frameworks.

It achieves this by ensuring that the application's core logic can be driven by, and drive, any external technology through a set of defined interfaces.


## **1. Core Principles**

The architecture is conceptually divided into two distinct zones: the **Interior** and the **Exterior**.


### **1.1 The Interior (The Domain Core)**

This is the application heart, often represented as the hexagon.

- **Business Logic:** Contains the application's true purpose (services, entities, use cases, and rules).

- **Decoupled:** It has no knowledge of how external agents (like a database or web framework) are implemented. It only defines _what_ it needs from them through interfaces.


### **1.2 The Exterior (External Agents)**

This zone contains all things outside the core, such as:

- Databases (PostgreSQL, MongoDB, etc.)

- User Interfaces (REST APIs, CLI, GUI)

- External Services (Email, Payment Gateways)

- Testing Frameworks


## **2. The Key Concept: Ports and Adapters**

The connection between the decoupled core and the messy outside world is managed by **Ports** and **Adapters**.


### **A. Ports (Interfaces/Contracts)**

A Port is simply an **interface** defined by the business logic (the Interior). Ports define the boundary of the application.

1. **Driving Port (Inbound Port):** An interface that the external user/client (the **Primary Adapter**) uses to talk _to_ the application core. This allows the core to be driven by a user interface, a message queue, a script, etc.

- _Example:_ An interface OrderPlacementService with a method placeOrder(details).

2. **Driven Port (Outbound Port):** An interface that the application core uses to talk _to_ external resources (the **Secondary Adapter**). This allows the core to fetch data or send messages.

- _Example:_ An interface UserRepository with a method save(user).


### **B. Adapters (Implementations)**

An Adapter is an implementation that translates the generic calls of the Port into specific technology calls.

1. **Primary Adapter (Driving Adapter):** Implements the Driving Port. It converts an external event (like an HTTP request) into a method call on the core business logic.

- _Examples:_ A **REST Controller**, a **CLI Command Handler**, or a **Message Queue Listener**.

2. **Secondary Adapter (Driven Adapter):** Implements the Driven Port. It converts the core's request (e.g., "save this user") into a specific technology instruction.

- _Examples:_ A **PostgreSQL Repository**, a **gRPC Client**, or a **JMS Sender**.


## **3. The Dependency Rule (The Arrow of Control)**

The most important rule in Hexagonal Architecture is the **Dependency Inversion Principle (DIP)**:

**Dependencies must always point inwards, toward the Core Domain.**

The Interior knows nothing about the Exterior. This means:

- The Application Core defines the Ports (interfaces).

- The Adapters (implementations) live in the Exterior and depend on the Ports defined in the Interior.

This architecture protects the core business logic from changes in technology. If you switch your database from MySQL to DynamoDB, only the **Secondary Adapter** needs to be changed; the Core Domain remains untouched.


## **4. Key Benefits**

1. **Testability:** You can easily test the entire business logic by providing _mock_ Primary and Secondary Adapters instead of needing a full database or network connection.

2. **Flexibility:** You can swap out any external agent (database, UI, messaging system) without rewriting the core application logic.

3. **Maintainability:** The clear separation ensures that business rules are never accidentally mixed with technological details, making the code easier to understand and maintain over time.
