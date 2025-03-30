import socket
import threading
import json
import time
import signal # Import signal module
import sys   # Import sys module for sys.exit

HOST = '0.0.0.0'
REGISTRY_PORT = 9999 # Port for clients to connect to the registry
MAX_CONNECTIONS = 10
BUFFER_SIZE = 4096 # Increased buffer size just in case

peer_lock = threading.Lock()
# Store: { peer_id (ip:p2p_port) : {'socket': client_socket, 'addr': client_address, 'p2p_port': p2p_port, 'name': name} }
connected_peers = {}

# --- Graceful Shutdown Signal Handling ---
shutdown_requested = False

def handle_shutdown_signal(sig, frame):
    """Sets the shutdown flag when SIGINT or SIGTERM is received."""
    global shutdown_requested
    if not shutdown_requested:
        print(f"\n[SHUTDOWN] Signal {sig} received. Initiating graceful shutdown...")
        shutdown_requested = True

# --- Broadcast and Peer List Functions ---

def broadcast_update(message_dict, exclude_peer_id=None):
    """Broadcasts updates to all peers, optionally excluding one."""
    with peer_lock:
        message_json = json.dumps(message_dict) + "\n"
        encoded_message = message_json.encode('utf-8')
        peers_to_remove = []
        # Use list() to avoid RuntimeError if dict changes size during iteration
        for peer_id, peer_info in list(connected_peers.items()):
            if peer_id == exclude_peer_id: # Skip excluded peer
                continue
            try:
                if peer_info and 'socket' in peer_info and peer_info['socket']:
                     peer_info['socket'].sendall(encoded_message)
                else:
                     print(f"[WARNING] Peer {peer_id} found in dict but has no valid socket during broadcast.")
                     peers_to_remove.append(peer_id)
            except (socket.error, BrokenPipeError, OSError) as e:
                print(f"[WARNING] Failed to send update to {peer_info.get('name', peer_id)}: {e}. Marking for removal.")
                peers_to_remove.append(peer_id)

        # Clean up peers that caused errors (outside the main sending loop)
        cleanup_occurred = False
        for peer_id_to_remove in peers_to_remove:
             if peer_id_to_remove in connected_peers:
                 peer_socket_to_close = connected_peers[peer_id_to_remove].get('socket')
                 peer_name = connected_peers[peer_id_to_remove].get('name', peer_id_to_remove)
                 del connected_peers[peer_id_to_remove] # Remove entry first
                 print(f"[SERVER] Auto-removed unresponsive peer {peer_name} ({peer_id_to_remove}). Total: {len(connected_peers)}")
                 cleanup_occurred = True
                 if peer_socket_to_close:
                     try:
                         peer_socket_to_close.close()
                     except socket.error:
                         pass
                 # Do NOT broadcast removal from here to avoid recursion/complex locking.
                 # The disconnect will be handled by the client handler's finally block,
                 # or a separate future cleanup mechanism if added.

def get_peer_list_dict():
    """Returns a dictionary of peer IDs and their info (IP, P2P port, name)."""
    with peer_lock:
        # Ensure peers have necessary info before including them
        return {
            peer_id: {
                'ip': info['addr'][0],
                'p2p_port': info['p2p_port'],
                'name': info.get('name', 'Registering...') # Include name
            }
            for peer_id, info in connected_peers.items()
            if info.get('p2p_port') is not None and info.get('name') is not None # Only include fully registered peers
        }

def is_name_taken(name_to_check):
    """Checks if a name is already used by a connected peer."""
    with peer_lock:
        for peer_info in connected_peers.values():
            if peer_info.get('name') and peer_info['name'].lower() == name_to_check.lower(): # Case-insensitive check
                return True
        return False

# --- Client Handling Function (Updated for Naming) ---

def handle_client(client_socket, client_address):
    """Handles registration (including name) and cleanup for one peer."""
    print(f"[NEW CONNECTION] Peer attempting to register from {client_address}")
    # Initialize peer_info with None for name and p2p_port
    peer_info = {'socket': client_socket, 'addr': client_address, 'p2p_port': None, 'name': None}
    peer_id = None # Will be set to ip:p2p_port
    registered_peer_name = None # Store the registered name for logging/cleanup
    registered_peer_id = None   # Store the registered ID for logging/cleanup

    try:
        client_socket.settimeout(20.0) # Slightly longer timeout for registration steps

        # 1. Ask for registration info (implicitly asking for name and p2p port now)
        client_socket.sendall(json.dumps({'type': 'request_p2p_port'}).encode('utf-8') + b'\n') # Still use old trigger

        # 2. Receive registration data (expecting type=register, p2p_port, name)
        buffer = ""
        registration_complete = False
        while not registration_complete and not shutdown_requested:
            try:
                data = client_socket.recv(BUFFER_SIZE)
                if not data:
                    print(f"[DISCONNECTED] Peer {client_address} disconnected before registering.")
                    return # Exit thread

                buffer += data.decode('utf-8')
                while '\n' in buffer and not registration_complete: # Process one message at a time until registered
                    message_json, buffer = buffer.split('\n', 1)
                    if not message_json.strip(): continue

                    try:
                        message = json.loads(message_json)
                        msg_type = message.get('type')
                        received_p2p_port = message.get('p2p_port')
                        received_name = message.get('name')

                        if msg_type == 'register' and received_p2p_port is not None and received_name:
                            # --- Validation ---
                            try:
                                p2p_port = int(received_p2p_port)
                                if not (0 < p2p_port < 65536):
                                    raise ValueError("Port out of range")
                            except (ValueError, TypeError):
                                print(f"[ERROR] Invalid P2P port '{received_p2p_port}' from {client_address}. Disconnecting.")
                                client_socket.sendall(json.dumps({'type': 'error', 'message': 'Invalid P2P port number'}).encode('utf-8') + b'\n')
                                return

                            clean_name = str(received_name).strip()
                            if not clean_name or len(clean_name) > 50: # Basic validation
                                print(f"[ERROR] Invalid name '{received_name}' from {client_address}. Disconnecting.")
                                client_socket.sendall(json.dumps({'type': 'error', 'message': 'Invalid name (empty or too long)'}).encode('utf-8') + b'\n')
                                return

                            # --- Check Name Uniqueness ---
                            if is_name_taken(clean_name):
                                print(f"[REJECTED] Name '{clean_name}' from {client_address} is already taken.")
                                client_socket.sendall(json.dumps({'type': 'error', 'message': f"Name '{clean_name}' is already taken."}).encode('utf-8') + b'\n')
                                return # Disconnect client

                            # --- Registration Success ---
                            peer_id = f"{client_address[0]}:{p2p_port}"
                            peer_info['p2p_port'] = p2p_port
                            peer_info['name'] = clean_name # Store the validated name
                            registered_peer_name = clean_name # Keep for finally block
                            registered_peer_id = peer_id    # Keep for finally block

                            print(f"[REGISTERED] Peer '{clean_name}' ({peer_id}) registered (P2P port: {p2p_port}).")

                            # Add to shared dict under lock
                            with peer_lock:
                                # Handle potential rapid reconnect/overwrite
                                if peer_id in connected_peers:
                                     print(f"[WARNING] Peer ID {peer_id} already exists (possibly old connection). Overwriting.")
                                     try:
                                         old_sock = connected_peers[peer_id].get('socket')
                                         if old_sock and old_sock != client_socket:
                                             old_sock.close()
                                     except socket.error: pass
                                connected_peers[peer_id] = peer_info
                                print(f"[SERVER] Total registered peers: {len(connected_peers)}")

                            # Send current peer list (including names) to the new peer
                            current_peers = get_peer_list_dict()
                            # Filter out self before sending
                            peers_for_client = {pid: pinfo for pid, pinfo in current_peers.items() if pid != peer_id}
                            client_socket.sendall(json.dumps({'type': 'peer_list', 'peers': peers_for_client}).encode('utf-8') + b'\n')

                            # Notify all *other* peers about the join
                            join_info = {'ip': client_address[0], 'p2p_port': p2p_port, 'name': clean_name}
                            broadcast_update({'type': 'peer_joined', 'id': peer_id, 'peer_info': join_info}, exclude_peer_id=peer_id)

                            registration_complete = True # Exit registration loops
                            # break # Exit inner while '\n' in buffer loop automatically due to outer loop condition

                        else:
                             # Received something other than a valid register message
                             print(f"[WARNING] Received unexpected/incomplete data from {client_address} during registration: {message_json}")
                             # Optionally send an error back

                    except json.JSONDecodeError:
                         print(f"[ERROR] Invalid JSON during registration from {client_address}: {message_json}")
                         return
                    except socket.error as e:
                         print(f"[ERROR] Socket error during registration processing for {client_address}: {e}")
                         return
                    except Exception as e:
                        print(f"[ERROR] Unexpected error processing registration message from {client_address}: {e}")
                        return

            except socket.timeout:
                if not registration_complete: # Only log timeout if registration didn't finish
                    print(f"[TIMEOUT] Peer {client_address} timed out during registration.")
                return # Exit thread
            except (socket.error, ConnectionResetError, BrokenPipeError) as e:
                 print(f"[ERROR] Socket error receiving registration data for {client_address}: {e}")
                 return
            except UnicodeDecodeError:
                 print(f"[ERROR] Received non-UTF8 data from {client_address} during registration. Disconnecting.")
                 return

        # End of registration loop

        if not registration_complete and not shutdown_requested:
             print(f"[WARNING] Peer {client_address} failed to complete registration properly.")
             return

        # --- Keep-alive Phase ---
        if registration_complete:
             client_socket.settimeout(None) # Disable timeout for normal operation

             while not shutdown_requested:
                 try:
                     # Check connection health by peeking. Recv(1) with MSG_PEEK is non-blocking if data available,
                     # blocks otherwise, and raises error on disconnect.
                     data = client_socket.recv(1, socket.MSG_PEEK)
                     if not data:
                         print(f"[DISCONNECTED] Peer '{registered_peer_name}' ({registered_peer_id}) disconnected gracefully.")
                         break
                     time.sleep(10) # Reduce frequency of checks
                 except (socket.timeout, InterruptedError):
                     continue # Ignore timeouts/interrupts, check shutdown_requested
                 except (ConnectionResetError, BrokenPipeError, socket.error) as e:
                     print(f"[DISCONNECTED] Peer '{registered_peer_name}' ({registered_peer_id}) connection error: {e}")
                     break
                 except Exception as e:
                     print(f"[ERROR] Unexpected error during keep-alive for '{registered_peer_name}' ({registered_peer_id}): {e}")
                     break

    # --- Exception handling for the whole client interaction ---
    except (ConnectionResetError, BrokenPipeError):
        # Use registered name/id if available, otherwise fall back to address
        peer_desc = f"'{registered_peer_name}' ({registered_peer_id})" if registered_peer_name else client_address
        print(f"[DISCONNECTED] Peer {peer_desc} connection reset/broken.")
    except socket.timeout:
         # This timeout is now specific to the initial registration phase
         peer_desc = f"'{registered_peer_name}' ({registered_peer_id})" if registered_peer_name else client_address
         print(f"[TIMEOUT] Peer {peer_desc} timed out.") # Could happen if timeout set before None
    except socket.error as e:
        peer_desc = f"'{registered_peer_name}' ({registered_peer_id})" if registered_peer_name else client_address
        print(f"[ERROR] Socket error with {peer_desc}: {e}")
    except Exception as e:
        peer_desc = f"'{registered_peer_name}' ({registered_peer_id})" if registered_peer_name else client_address
        print(f"[ERROR] Unhandled error in handle_client for {peer_desc}: {e}")

    finally:
        # --- Cleanup ---
        removed = False
        peer_left_id = registered_peer_id # Use the stored ID
        peer_left_name = registered_peer_name # Use the stored name

        if peer_left_id and peer_left_name: # Only cleanup/broadcast if registration was successful
            with peer_lock:
                if peer_left_id in connected_peers:
                    # Check if the socket matches in case of overwrite scenarios
                    if connected_peers[peer_left_id]['socket'] == client_socket:
                         del connected_peers[peer_left_id]
                         removed = True
                         print(f"[REMOVED] Peer '{peer_left_name}' ({peer_left_id}) removed. Total registered peers: {len(connected_peers)}")
                    else:
                         # Socket doesn't match, means this entry was overwritten. Don't broadcast leave.
                          print(f"[INFO] Socket for {peer_left_name} ({peer_left_id}) closed, but registry entry was already overwritten.")
                else:
                     # Peer ID/Name existed but already removed (e.g., by broadcast error cleanup)
                     print(f"[INFO] Peer '{peer_left_name}' ({peer_left_id}) already removed from registry during final cleanup.")
        else:
             # Never fully registered
             print(f"[CLEANUP] Unregistered peer {client_address} connection closed.")

        # Close the socket outside the lock
        try:
            client_socket.shutdown(socket.SHUT_RDWR) # Signal close intention
        except (socket.error, OSError):
            pass # Ignore if already closed/invalid
        try:
            client_socket.close()
        except (socket.error, OSError):
            pass # Ignore errors closing socket

        # Broadcast leave update *after* lock is released and socket is closed
        # Only broadcast if removed successfully from the primary dictionary
        if removed and peer_left_id:
            # Broadcast requires ID. Name is useful for clients but not strictly needed in the message here.
            broadcast_update({'type': 'peer_left', 'id': peer_left_id})


# --- Main Server Function (Updated for Shutdown) ---
def start_registry_server():
    global shutdown_requested

    try:
        signal.signal(signal.SIGINT, handle_shutdown_signal)
        signal.signal(signal.SIGTERM, handle_shutdown_signal)
    except ValueError:
        print("[WARNING] Cannot set signal handlers (e.g., not main thread).")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.settimeout(1.0) # Check for shutdown flag periodically

    print("[STARTING] P2P Registry Server is starting...")

    try:
        server_socket.bind((HOST, REGISTRY_PORT))
        print(f"[LISTENING] Registry listening on {HOST}:{REGISTRY_PORT}")
        server_socket.listen(MAX_CONNECTIONS)

        while not shutdown_requested:
            try:
                client_socket, client_address = server_socket.accept()
                # Timeout inherited initially, will be changed by handler

                client_thread = threading.Thread(
                    target=handle_client,
                    args=(client_socket, client_address),
                    daemon=True
                )
                client_thread.start()

            except socket.timeout:
                continue # Normal condition to check shutdown_requested
            except OSError as e:
                 if shutdown_requested:
                      print("[INFO] Server socket closed during shutdown.")
                      break
                 else:
                      print(f"[ERROR] Error accepting connection (OS Error): {e}")
                      time.sleep(1)
            except Exception as e:
                if not shutdown_requested:
                    print(f"[ERROR] Unexpected error accepting connection: {e}")
                time.sleep(1)

    except Exception as e:
        if not shutdown_requested:
            print(f"[ERROR] Server encountered a fatal error: {e}")
            shutdown_requested = True # Trigger shutdown process

    finally:
        # --- Server Shutdown Cleanup ---
        print("[CLEANUP] Server shutdown sequence initiated.")
        print("[CLEANUP] Closing listening socket...")
        server_socket.close()

        print("[CLEANUP] Notifying and closing remaining peer connections...")
        with peer_lock:
            # Copy items for safe iteration while clearing
            peers_to_notify = list(connected_peers.items())
            connected_peers.clear() # Clear the main list

        # Notify peers outside the lock
        shutdown_message = json.dumps({'type': 'server_shutdown', 'message': 'Server is shutting down.'}) + "\n"
        encoded_shutdown_message = shutdown_message.encode('utf-8')
        closed_count = 0

        for peer_id, peer_info in peers_to_notify:
             sock = peer_info.get('socket')
             peer_name = peer_info.get('name', peer_id) # Use name in log
             if sock:
                 print(f"[CLEANUP] Closing connection to {peer_name} ({peer_id})...")
                 try:
                     sock.settimeout(0.5) # Short timeout for sending notification
                     sock.sendall(encoded_shutdown_message)
                     # time.sleep(0.05) # Short delay
                     sock.shutdown(socket.SHUT_RDWR)
                 except (socket.error, OSError):
                     # print(f"[CLEANUP] Error notifying peer {peer_name} (might be already closed).")
                     pass # Ignore errors during shutdown notification
                 finally:
                     try:
                         sock.close()
                         closed_count += 1
                     except (socket.error, OSError):
                         pass # Ignore errors closing socket

        print(f"[STOPPED] Server has stopped. {closed_count} active connection(s) closed.")

# --- Entry Point ---
if __name__ == "__main__":
    start_registry_server()
    print("[EXIT] Main thread exiting.")
    sys.exit(0) # Ensure clean exit code