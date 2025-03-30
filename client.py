import socket
import threading
import sys
import time
import json
import random

# --- Configuration ---
REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 9999
MY_P2P_PORT = random.randint(10000, 60000)
BUFFER_SIZE = 4096

# --- Shared State ---
peer_list_lock = threading.Lock()
# Stores { peer_id: {'ip': ip, 'p2p_port': port, 'name': name} }
# peer_id is the key assigned by the server (e.g., ip:port string or a unique ID)
known_peers = {}
running = True
MY_NAME = None # Store the client's chosen name

# --- Helper Functions ---
def get_peer_name_by_address(ip, port):
    """Looks up the name of a peer given their listening IP and P2P port."""
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            # Check against the peer's listening address info
            if info.get('ip') == ip and info.get('p2p_port') == port:
                return info.get('name', f"{ip}:{port}") # Fallback to ip:port if name missing
    return f"{ip}:{port}" # Fallback if peer not found

def get_peer_info_by_name(name):
    """Looks up the connection info (ip, port) of a peer by their name."""
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('name') == name:
                return info # Return the whole info dict
    return None

# --- P2P Listener Thread ---
def handle_peer_connection(peer_socket, peer_address):
    """Handles an incoming connection from another peer."""
    # Look up the name based on the connection address
    peer_name = get_peer_name_by_address(peer_address[0], peer_address[1])
    # print(f"\n[P2P <<] Connection from {peer_name} ({peer_address[0]}:{peer_address[1]})")
    try:
        while True:
            data = peer_socket.recv(BUFFER_SIZE)
            if not data:
                # print(f"\n[P2P <<] Peer {peer_name} disconnected.")
                break
            message = data.decode('utf-8')
            # Use the looked-up name in the output
            print(f"\r{' ' * 60}\r[P2P MSG from {peer_name}]: {message}\n> ", end="", flush=True)
            # response = f"Received: {message}" # Example response
            # peer_socket.sendall(response.encode('utf-8'))
    except ConnectionResetError:
        print(f"\n[P2P <<] Peer {peer_name} connection reset.")
    except Exception as e:
        print(f"\n[P2P ERROR] Error with peer {peer_name}: {e}")
    finally:
        peer_socket.close()

def listen_for_peers(host, port):
    """Starts a server socket to listen for incoming P2P connections."""
    p2p_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    p2p_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        p2p_server_socket.bind((host, port))
        p2p_server_socket.listen(5)
        print(f"[P2P LISTENING] Ready for P2P connections on {host}:{port}")

        while running:
            try:
                p2p_server_socket.settimeout(1.0)
                peer_socket, peer_address = p2p_server_socket.accept()
                p2p_server_socket.settimeout(None)

                peer_thread = threading.Thread(
                    target=handle_peer_connection,
                    args=(peer_socket, peer_address),
                    daemon=True)
                peer_thread.start()

            except socket.timeout:
                continue
            except socket.error as e:
                if running:
                     print(f"[P2P LISTENER ERROR] {e}")
                break
            except Exception as e:
                 print(f"[P2P LISTENER CRITICAL ERROR] {e}")
                 break

    finally:
        print("[P2P LISTENING] Shutting down P2P listener.")
        p2p_server_socket.close()

# --- Server Communication Thread ---
def listen_to_registry(registry_socket):
    """Listens for updates from the central registry server."""
    global known_peers, running, MY_NAME # Make sure MY_NAME is accessible
    buffer = ""
    prompt = "> "
    while running:
        try:
            data = registry_socket.recv(BUFFER_SIZE)
            if not data:
                print("\n[CONNECTION LOST] Registry server closed the connection.")
                running = False
                break
            buffer += data.decode('utf-8')

            while '\n' in buffer:
                 message_json, buffer = buffer.split('\n', 1)
                 if not message_json: continue

                 try:
                     message = json.loads(message_json)
                     msg_type = message.get('type')

                     if msg_type == 'request_p2p_port':
                         # Server is asking for our info, send name and P2P port
                         if MY_NAME: # Ensure name has been set
                             registration_msg = json.dumps({
                                 'type': 'register',
                                 'p2p_port': MY_P2P_PORT,
                                 'name': MY_NAME # Include the name
                             }) + "\n"
                             registry_socket.sendall(registration_msg.encode('utf-8'))
                             print(f"\n[REGISTRY] Registered as '{MY_NAME}' with P2P port {MY_P2P_PORT}.")
                         else:
                             print("\n[ERROR] Cannot register, name not set yet.")
                             # Maybe request name again or shut down? For now, just log.

                     elif msg_type == 'peer_list':
                         print("\n[REGISTRY] Received initial peer list.")
                         with peer_list_lock:
                             # Expecting peers format: { server_id: {'ip': ..., 'p2p_port': ..., 'name': ...}, ... }
                             known_peers = message.get('peers', {})
                             # Remove self from list if present (server might include it)
                             my_server_id = None
                             for pid, info in list(known_peers.items()): # Iterate over copy for safe removal
                                 if info.get('name') == MY_NAME:
                                     my_server_id = pid
                                     break
                             if my_server_id and my_server_id in known_peers:
                                 del known_peers[my_server_id]

                         # Optional: Print the list using names
                         # print("[KNOWN PEERS]:")
                         # with peer_list_lock:
                         #    if known_peers:
                         #        for pid, info in known_peers.items():
                         #            print(f"  - {info.get('name', 'Unknown')} ({pid})")
                         #    else:
                         #        print("  None")


                     elif msg_type == 'peer_joined':
                         peer_id = message.get('id')
                         peer_info = message.get('peer_info') # Expecting {'ip':..., 'p2p_port':..., 'name':...}
                         peer_name = peer_info.get('name', 'Unknown') if peer_info else 'Unknown'
                         if peer_id and peer_info and peer_name != MY_NAME: # Don't add self
                             with peer_list_lock:
                                 known_peers[peer_id] = peer_info
                             print(f"\r{' ' * 60}\r[REGISTRY] Peer joined: {peer_name} ({peer_id})\n{prompt}", end="", flush=True)

                     elif msg_type == 'peer_left':
                         peer_id = message.get('id')
                         if peer_id:
                             removed_name = "Unknown"
                             with peer_list_lock:
                                 if peer_id in known_peers:
                                     removed_name = known_peers[peer_id].get('name', 'Unknown')
                                     del known_peers[peer_id]
                             print(f"\r{' ' * 60}\r[REGISTRY] Peer left: {removed_name} ({peer_id})\n{prompt}", end="", flush=True)

                     else:
                         print(f"\r{' ' * 60}\r[REGISTRY MSG] {message_json}\n{prompt}", end="", flush=True)

                 except json.JSONDecodeError:
                     print(f"\r{' ' * 60}\r[REGISTRY ERROR] Invalid JSON: {message_json}\n{prompt}", end="", flush=True)
                 except socket.error as e:
                     if running:
                         print(f"\n[ERROR] Socket error sending registration: {e}")
                     running = False
                     break
                 except Exception as e:
                      print(f"\n[ERROR] Error processing registry message: {e}")


        except ConnectionResetError:
            print("\n[CONNECTION LOST] Connection to registry was reset.")
            running = False
            break
        except ConnectionAbortedError:
             print("\n[CONNECTION CLOSED] Connection to registry closed.")
             running = False
             break
        except OSError as e:
            if running:
                print(f"\n[SOCKET ERROR] Registry listener error: {e}")
            running = False
            break
        except Exception as e:
            print(f"\n[ERROR] Unexpected error receiving from registry: {e}")
            running = False
            break

    print("Registry listener thread finished.")
    running = False

# --- Function to send P2P message ---
def send_p2p_message(target_name, message):
    """Connects directly to a peer identified by name and sends a message."""
    target_info = get_peer_info_by_name(target_name) # Find peer by name

    if not target_info:
        print(f"[P2P ERROR] Peer '{target_name}' not found in known peers.")
        return

    target_ip = target_info.get('ip')
    target_port = target_info.get('p2p_port')

    if not target_ip or not target_port:
        print(f"[P2P ERROR] Peer '{target_name}' has incomplete information (missing IP or Port).")
        return

    print(f"[P2P >>] Attempting to send to {target_name} ({target_ip}:{target_port})...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_socket:
             peer_socket.settimeout(5.0)
             peer_socket.connect((target_ip, target_port))
             peer_socket.sendall(message.encode('utf-8'))
             print(f"[P2P >>] Message sent successfully to {target_name}.")
    except socket.timeout:
         print(f"[P2P ERROR] Connection to {target_name} timed out.")
    except ConnectionRefusedError:
         print(f"[P2P ERROR] Connection to {target_name} refused (peer might be offline or firewall).")
    except socket.error as e:
         print(f"[P2P ERROR] Socket error sending to {target_name}: {e}")
    except Exception as e:
         print(f"[P2P ERROR] Unexpected error sending to {target_name}: {e}")


# --- Main Client Function ---
def start_p2p_client():
    global running, known_peers, MY_NAME

    # 0. Get User's Name
    while not MY_NAME:
        try:
            name_input = input("Enter your desired name: ").strip()
            if name_input: # Basic validation: not empty
                MY_NAME = name_input
            else:
                print("Name cannot be empty.")
        except EOFError:
            print("\nExiting.")
            return
        except KeyboardInterrupt:
             print("\nExiting.")
             return


    # 1. Start P2P listener
    p2p_listener_thread = threading.Thread(target=listen_for_peers, args=('0.0.0.0', MY_P2P_PORT), daemon=True)
    p2p_listener_thread.start()
    time.sleep(0.1)

    # 2. Connect to the Registry Server
    registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        print(f"[CONNECTING] Connecting to Registry Server {REGISTRY_HOST}:{REGISTRY_PORT}...")
        registry_socket.connect((REGISTRY_HOST, REGISTRY_PORT))
        print("[CONNECTED] Connected to the Registry.")

        # 3. Start thread to listen for Registry updates
        # Note: Registration now happens when server sends 'request_p2p_port'
        registry_listener_thread = threading.Thread(target=listen_to_registry, args=(registry_socket,), daemon=True)
        registry_listener_thread.start()

        # 4. Main loop for user commands
        print(f"\nWelcome, {MY_NAME}!")
        print("Commands:")
        print("  /list              - Show known peers by name")
        print("  /msg <name> <message> - Send direct message (e.g., /msg Alice Hello there!)")
        print("  /quit              - Exit")

        while running:
            try:
                 cmd = input("> ")
            except EOFError:
                 cmd = "/quit"

            if not running: break

            if cmd.lower() == '/quit':
                running = False
                break
            elif cmd.lower() == '/list':
                with peer_list_lock:
                    if known_peers:
                         print("[KNOWN PEERS]:")
                         # Sort peers by name for consistent display
                         sorted_peers = sorted(known_peers.values(), key=lambda x: x.get('name', ''))
                         for info in sorted_peers:
                              peer_name = info.get('name', 'Unknown')
                              peer_id = next((pid for pid, pinfo in known_peers.items() if pinfo == info), "N/A") # Find ID
                              print(f"  - {peer_name}  (ID: {peer_id})") # Display name and maybe ID
                    else:
                         print("[KNOWN PEERS]: None")
            elif cmd.startswith('/msg '):
                parts = cmd.split(' ', 2)
                if len(parts) == 3:
                    peer_name, message = parts[1], parts[2]
                    if peer_name == MY_NAME:
                        print("[CMD ERROR] Cannot send message to yourself.")
                    else:
                        send_p2p_message(peer_name, message) # Use name here
                else:
                    print("[CMD ERROR] Usage: /msg <name> <message>")
            elif cmd.strip():
                 print("[CMD ERROR] Unknown command. Use /list, /msg, or /quit.")

    except socket.error as e:
        print(f"[ERROR] Could not connect to registry server: {e}")
        running = False
    except KeyboardInterrupt:
        print("\n[EXITING] You interrupted the client.")
        running = False
    finally:
        running = False
        print("[CLOSING] Closing connection to registry...")
        try:
            # Optional: Send a disconnect message to the server?
            # Example: disconnect_msg = json.dumps({'type': 'disconnect'}) + "\n"
            # registry_socket.sendall(disconnect_msg.encode('utf-8'))
            registry_socket.close()
        except Exception: # Catch broad exceptions during cleanup
            pass

        print("[CLOSED] Client shutting down.")
        time.sleep(0.5)


# --- Entry Point ---
if __name__ == "__main__":
    start_p2p_client()