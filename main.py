"""Examples demonstrating peppylib topic, service, and action APIs."""

import asyncio
import signal

import peppylib
from peppylib import ActionMessenger, MessengerHandle, ServiceMessenger, TopicMessenger
from peppylib.config import DEFAULT_MESSAGING_PORT, QoSProfile
from peppylib.names import generate_name

HOST = "localhost"
PORT = DEFAULT_MESSAGING_PORT
DAEMON_NODE = "test-daemon"
TIMEOUT = 3.0


async def topic_example() -> None:
    """Publish a message to a topic and receive it on a subscriber."""
    instance_id = generate_name()
    topic_name = "greetings"

    publisher = await MessengerHandle.from_host_port(HOST, PORT)
    subscriber = await MessengerHandle.from_host_port(HOST, PORT)

    # Subscribe first so we don't miss the message
    subscription = await TopicMessenger.subscribe(
        subscriber,
        DAEMON_NODE,
        instance_id,
        "topic-publisher",
        topic_name,
        None,  # target_daemon_node
        None,  # target_instance_id
        QoSProfile.Reliable,
    )

    # Publish a message
    await TopicMessenger.emit(
        publisher,
        DAEMON_NODE,
        instance_id,
        "topic-publisher",
        topic_name,
        QoSProfile.Reliable,
        b"Hello from topic publisher!",
    )

    # Receive the message
    msg = await asyncio.wait_for(subscription.on_next_message(), timeout=TIMEOUT)
    if msg is not None:
        print(f"[topic] Received: {msg.payload.decode('utf-8')}")
    else:
        print("[topic] Subscription closed")


async def service_example() -> None:
    """Expose a service and call it from a client."""
    instance_id = generate_name()
    service_name = "echo"

    server_handle = await MessengerHandle.from_host_port(HOST, PORT)
    client_handle = await MessengerHandle.from_host_port(HOST, PORT)

    # Start listening for requests
    service = await ServiceMessenger.listen(
        server_handle,
        DAEMON_NODE,
        instance_id,
        "service-server",
        service_name,
    )

    async def handle_request(request) -> bytes:
        text = request.payload.decode("utf-8")
        print(f"[service] Server received: {text}")
        return f"Echo: {text}".encode("utf-8")

    # Send a request from the client (concurrently with the server handling it)
    async def send_request() -> None:
        response = await ServiceMessenger.poll(
            client_handle,
            DAEMON_NODE,
            instance_id,
            "service-server",
            service_name,
            None,  # target_daemon_node
            None,  # target_instance_id
            b"Hello from service client!",
            TIMEOUT,
        )
        print(f"[service] Client received: {response.payload.decode('utf-8')}")

    # Run server and client concurrently
    # handle_next_request returns a PyO3 Future, so wrap in async function for create_task
    async def serve() -> None:
        await service.handle_next_request(handle_request)

    server_task = asyncio.create_task(serve())
    client_task = asyncio.create_task(send_request())
    await asyncio.gather(server_task, client_task)


async def action_example() -> None:
    """Expose an action server and send a goal from a client."""
    instance_id = generate_name()
    action_name = "compute"

    server_handle = await MessengerHandle.from_host_port(HOST, PORT)
    client_handle = await MessengerHandle.from_host_port(HOST, PORT)

    # Expose the action (server side)
    action = await ActionMessenger.expose(
        server_handle,
        DAEMON_NODE,
        instance_id,
        "action-server",
        action_name,
    )

    # Server handles goal → publishes feedback → handles result sequentially
    async def server() -> None:
        await action.goal_service.handle_next_request(
            lambda request: f"Goal accepted: {request.payload.decode('utf-8')}".encode("utf-8")
        )
        print("[action] Server handled goal")

        await action.feedback_publisher.publish(b"Working on it...")
        print("[action] Server sent feedback")

        await action.result_service.handle_next_request(
            lambda _request: b"Computation complete!"
        )
        print("[action] Server sent result")

    # Client sends a goal
    async def send_goal() -> None:
        goal_handle = await ActionMessenger.send_goal(
            client_handle,
            DAEMON_NODE,
            instance_id,
            "action-server",
            action_name,
            None,  # target_daemon_node
            None,  # target_instance_id
            b"Compute something",
            QoSProfile.Reliable,
            TIMEOUT,
        )
        goal_text = goal_handle.goal_response.payload.decode("utf-8")
        print(f"[action] Client goal response: {goal_text}")

        # Receive feedback
        feedback = await asyncio.wait_for(
            goal_handle.on_next_feedback(), timeout=TIMEOUT
        )
        print(f"[action] Client feedback: {feedback.payload.decode('utf-8')}")

        # Request result
        result = await ActionMessenger.request_result(
            client_handle, goal_handle, TIMEOUT
        )
        print(f"[action] Client result: {result.payload.decode('utf-8')}")

    # Run server and client concurrently
    server_task = asyncio.create_task(server())
    client_task = asyncio.create_task(send_goal())
    await asyncio.gather(server_task, client_task)


async def main() -> None:
    print(f"peppylib version: {peppylib.__version__}\n")

    print("--- Topic Example ---")
    await topic_example()

    print("\n--- Service Example ---")
    await service_example()

    print("\n--- Action Example ---")
    await action_example()


if __name__ == "__main__":
    asyncio.run(main())
