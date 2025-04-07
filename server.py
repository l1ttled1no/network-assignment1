# --- p2p_registry.py ---
import socket
import threading
import json
import time
import sys
import uuid

# --- Configuration ---
HOST = '0.0.0.0'
PORT = 9999
BUFFER_SIZE = 4096

# --- Shared State ---
peer_lock = threading.Lock()
# Store logged-in users: { peer_id: {'socket': s, 'addr': a, 'p2p_port': p, 'name': n, 'id': user_id, 'guest': False} }
logged_in_peers = {}
# Store guest users: { peer_id: {'socket': s, 'addr': a, 'p2p_port': p, 'name': n, 'guest': True} }
guest_peers = {}
# Store registered channels: { channel_name: {'owner_id': pid, 'owner_name': n, ...} }
channel_lock = threading.Lock()
registered_channels = {}

# User database (simple example - replace with actual DB/file storage)
user_credentials_lock = threading.Lock()
# { user_id: {'name': name, 'password_hash': ...} } # In real app, use hashed passwords!
# For this example, we'll just check ID and Name match if user exists
registered_users_db = {

    # Add more pre-registered users if needed for testing
}


shutdown_requested = False

# --- Helper Functions ---

def send_message_to_peer(peer_id, message_dict):
    """Sends a JSON message to a specific peer (guest or logged-in)."""
    target_peer_info = None
    with peer_lock:
        target_peer_info = logged_in_peers.get(peer_id) or guest_peers.get(peer_id)
    # print(target_peer_info)
    if target_peer_info and target_peer_info.get('socket'):
        try:
            msg_json = json.dumps(message_dict) + "\n"
            target_peer_info['socket'].sendall(msg_json.encode('utf-8'))
            # print(f"[DEBUG SEND to {peer_id}] {msg_json.strip()}") # Debugging
            return True
        except (socket.error, OSError) as e:
            print(f"[ERROR] Failed to send message to {peer_id} ({target_peer_info.get('name')}): {e}")
            # Consider marking peer for removal here if send fails repeatedly
            return False
    # else: print(f"[WARN] Cannot send message, peer {peer_id} not found or socket missing.")
    return False

def send_error_to_peer(peer_id, error_message):
    """Sends a structured error message."""
    print(f"[SEND ERROR to {peer_id}]: {error_message}")
    send_message_to_peer(peer_id, {'type': 'error', 'message': error_message})

def broadcast_update(message_dict, exclude_peer_id=None):
    """Sends a message to all *logged-in* peers, optionally excluding one."""
    # Decide if guests should receive certain broadcasts (e.g., new channels)
    # For now, only broadcast to logged-in peers.
    with peer_lock:
        # Create a list of peer IDs to avoid issues if dict changes during iteration
        peer_ids = list(logged_in_peers.keys())
        peer_ids.extend(list(guest_peers.keys()))

    for pid in peer_ids:
        if pid != exclude_peer_id:
            send_message_to_peer(pid, message_dict)

def is_guest_name_taken(name):
    """Checks if a guest name is currently in use."""
    with peer_lock:
        for peer_info in guest_peers.values():
            if peer_info.get('name') == name:
                return True
    return False

def is_logged_in_name_taken(name):
    """Checks if a name is used by a currently logged-in user."""
    with peer_lock:
        for peer_info in logged_in_peers.values():
            if peer_info.get('name') == name:
                return True
    return False

def validate_user_login(user_id, name):
    """
    Checks if the login credentials are valid against the 'DB'.
    Returns: (bool valid, str error_message or None)
    """
    with user_credentials_lock:
        if user_id in registered_users_db:
            # Simple check: ID exists and name matches stored name
            if registered_users_db[user_id]['name'] == name:
                return True, None # Valid login
            else:
                return False, f"Name '{name}' does not match ID '{user_id}'."
        else:
            # Allow first-time login? Or reject unknown IDs?
            # For now, let's allow first-time registration via login command
            # Check if the desired name is already associated with *another* ID
            registered_users_db[user_id] = {'name': name}
            return True, None # Treat as valid first-time login

def register_new_user(user_id, name):
    """Adds a new user to the 'DB'. Assumes validation already passed."""
    with user_credentials_lock:
        if user_id not in registered_users_db:
            registered_users_db[user_id] = {'name': name}
            print(f"[AUTH] Registered new user in DB: ID={user_id}, Name={name}")
            return True
        else:
            # Should not happen if validate_user_login is correct, but good failsafe
            print(f"[AUTH WARN] Attempted to re-register existing user ID: {user_id}")
            return False # Or just return True as they exist


def get_peers_dict():
    """Returns a serializable dict of currently logged-in peers' info."""
    peers_dict = {}
    with peer_lock:
        for pid, info in logged_in_peers.items():
            peers_dict[pid] = {
                'name': info.get('name'),
                'ip': info.get('addr', ('N/A', None))[0], # Get IP from address tuple
                'p2p_port': info.get('p2p_port'),
                'is_invisible': info.get('is_invisible', False)  # Add invisible status
            }
        for pid, info in guest_peers.items():
            peers_dict[pid] = {
                'name': info.get('name'),
                'ip': info.get('addr', ('N/A', None))[0], # Get IP from address tuple
                'p2p_port': info.get('p2p_port'),
                'is_invisible': info.get('is_invisible', False)  # Add invisible status
            }
    return peers_dict

# --- Main Client Handler (Modified Registration) ---
def handle_client(client_socket, client_address):
    """Handles initial login/guest, ongoing messages, and cleanup."""
    client_ip = client_address[0]
    client_registry_port = client_address[1]
    print(f"[NEW CONNECTION] Connection attempt from {client_ip}:{client_registry_port}")

    # peer_info will be populated after successful login/guest registration
    peer_info = None
    peer_id = None # Will be set on successful registration
    session_id = None  # Create a session id for each peer login
    registered_peer_name = None # Keep track for logging/cleanup
    is_client_guest = None # Keep track for cleanup
    buffer = ""

    try:
        # --- NEW: INITIAL LOGIN/GUEST PHASE ---
        # Expecting 'guest_login' or 'user_login' immediately
        client_socket.settimeout(30.0) # Timeout for receiving the first message
        initial_message_received = False
        registration_success = False

        while not initial_message_received and not shutdown_requested:
            try:
                data = client_socket.recv(BUFFER_SIZE)
                if not data:
                    print(f"[DISCONNECTED] {client_address} disconnected before sending login info.")
                    return # Close connection

                buffer += data.decode('utf-8')
                if '\n' in buffer:
                    message_json, buffer = buffer.split('\n', 1)
                    if not message_json.strip(): continue

                    initial_message_received = True # Process this message, then exit loop

                    try:
                        message = json.loads(message_json)
                        msg_type = message.get('type')
                        received_name = message.get('name')
                        received_ip = message.get('ip') # IP reported by client (use for info, maybe validation)
                        received_p2p_port = message.get('p2p_port')
                        received_id = message.get('id') # Only for 'user_login'

                        print(f"[RECV LOGIN] Type: {msg_type}, Name: {received_name}, IP: {received_ip}, P2P Port: {received_p2p_port}, ID: {received_id}")

                        # --- Validate Common Fields ---
                        try:
                            p2p_port = int(received_p2p_port); assert 1024 <= p2p_port <= 65535
                        except:
                            send_error_to_peer(None, f"Invalid P2P port: {received_p2p_port}"); time.sleep(0.1); return # Can't use peer_id yet
                        clean_name = str(received_name).strip()
                        if not clean_name or len(clean_name) > 50 or ' ' in clean_name:
                            send_error_to_peer(None, f"Invalid name format: '{received_name}'."); time.sleep(0.1); return

                        # Generate potential peer ID (might change if user logs in with existing session ID?)
                        # Using client's reported IP/port for now, but relying on registry IP is better.
                        # Let's use client's *registry* connection info as a unique temporary ID first
                        temp_peer_id = f"{client_ip}:{client_registry_port}" # Use connection source as temporary ID before registration

                        # --- Handle Guest Login ---
                        if msg_type == 'guest_login':
                            if is_guest_name_taken(clean_name) or is_logged_in_name_taken(clean_name):
                                send_error_to_peer(temp_peer_id, f"Name '{clean_name}' is currently in use.")
                                time.sleep(0.1); return

                            # Guest registration successful
                            peer_id = f"guest_{p2p_port}" # Generate a guest-specific ID
                            is_client_guest = True
                            peer_info = {
                                'socket': client_socket, 'addr': client_address,
                                'p2p_port': p2p_port, 'name': clean_name,
                                'guest': True,
                                'is_invisible': message.get('is_invisible', False)  # Add invisible status
                            }
                            with peer_lock:
                                guest_peers[peer_id] = peer_info
                            registered_peer_name = clean_name
                            registration_success = True
                            print(f"[REGISTERED GUEST] '{clean_name}' ({peer_id}) from {client_address}.")

                            # Send Ack
                            ack_msg = {'type': 'guest_ack', 'message': f"Registered as Guest '{clean_name}'.", 'client_ip': client_ip}
                            send_message_to_peer(peer_id, ack_msg)

                            # Send Peer List 
                            current_peers_dict = get_peers_dict()
                            print(current_peers_dict)
                            peers_for_client = {
                                pid: pinfo 
                                for pid, pinfo in current_peers_dict.items() 
                                if pid != peer_id and not pinfo.get('is_invisible', False)
                            }
                            send_message_to_peer(peer_id, {'type': 'peer_list', 'peers': peers_for_client})

                            # Broadcast Join (only for logged-in users)
                            join_info = {
                                'ip': client_ip, 
                                'p2p_port': p2p_port, 
                                'name': clean_name,
                                'is_invisible': peer_info.get('is_invisible', False)
                            } # Include invisible status
                            broadcast_update({'type': 'peer_joined', 'id': peer_id, 'peer_info': join_info}, exclude_peer_id=peer_id)

                        # --- Handle User Login ---
                        elif msg_type == 'user_login':
                            if not received_id or not str(received_id).isdigit():
                                send_error_to_peer(temp_peer_id, "User login requires a numeric ID."); time.sleep(0.1); return

                            user_id = str(received_id)

                            # Check if name is used by *another* logged-in user
                            # with peer_lock:
                            #     for pid, info in logged_in_peers.items():
                            #         if info.get('name') == clean_name
                            #         if info.get('name') == clean_name and info.get('id') != user_id:
                            #             send_error_to_peer(temp_peer_id, f"Name '{clean_name}' is in use by another logged-in user."); time.sleep(0.1); return
                            #         # Check if ID is used by *another* logged-in user (duplicate login attempt?)
                            #         if info.get('id') == user_id and info.get('socket') != client_socket:
                            #             # Handle duplicate login - e.g., disconnect old session? Deny new?
                            #             send_error_to_peer(temp_peer_id, f"User ID '{user_id}' is already logged in. Disconnect other session first."); time.sleep(0.1); return

                            # Check against "DB"
                            valid_creds, error_msg = validate_user_login(user_id, clean_name)
                            # print('Else')
                            if not valid_creds:
                                send_error_to_peer(temp_peer_id, f"Login failed: {error_msg}"); time.sleep(0.3); return

                             # --- Login Success ---
                             # Register new user in DB if they didn't exist (first login)
                            # with user_credentials_lock:
                            #     if user_id not in registered_users_db:
                            #         register_new_user(user_id, clean_name)

                            peer_id = f"user_{client_ip}:{p2p_port}" # Generate user-specific ID
                            is_client_guest = False
                            peer_info = {
                                'socket': client_socket, 'addr': client_address,
                                'p2p_port': p2p_port, 'name': clean_name,
                                'id': user_id, # Store the validated user ID
                                'guest': False,
                                'is_invisible': message.get('is_invisible', False)  # Add invisible status
                            }
                            with peer_lock:
                                logged_in_peers[peer_id] = peer_info
                            registered_peer_name = clean_name
                            registration_success = True
                            print(f"[REGISTERED USER] '{clean_name}' (ID: {user_id}, PeerID: {peer_id}) from {client_address}.")

                            # Send Ack
                            ack_msg = {'type': 'login_ack', 'message': f"Logged in as User '{clean_name}'.", 'client_ip': client_ip}
                            send_message_to_peer(peer_id, ack_msg)

                            # Send Peer List (only to logged-in users)
                            current_peers_dict = get_peers_dict()
                            peers_for_client = {
                                pid: pinfo 
                                for pid, pinfo in current_peers_dict.items() 
                                if pid != peer_id and not pinfo.get('is_invisible', False)
                            } # Exclude self and invisible peers
                            send_message_to_peer(peer_id, {'type': 'peer_list', 'peers': peers_for_client})

                            # Broadcast Join (only for logged-in users)
                            join_info = {
                                'ip': client_ip, 
                                'p2p_port': p2p_port, 
                                'name': clean_name,
                                'is_invisible': peer_info.get('is_invisible', False)
                            } # Include invisible status
                            broadcast_update({'type': 'peer_joined', 'id': peer_id, 'peer_info': join_info}, exclude_peer_id=peer_id)

                        # --- Handle 'request_channel_list' Request ---
                        elif msg_type == 'request_channel_list':
                            print(f"[REQUEST] Peer '{registered_peer_name}' requested channel list.")
                            channels_payload = {}
                            with channel_lock:
                                if is_client_guest:
                                    channels_payload = { 
                                        name: {
                                            'owner_name': info.get('owner_name', 'N/A'), 
                                            'owner_ip': info.get('owner_ip', 'N/A'), 
                                            'owner_p2p_port': info.get('owner_p2p_port', 'N/A')
                                        } 
                                        for name, info in registered_channels.items()
                                        if info.get('channel_type') == 'public'
                                    }
                                else:
                                    channels_payload = { 
                                        name: {
                                            'owner_name': info.get('owner_name', 'N/A'), 
                                            'owner_ip': info.get('owner_ip', 'N/A'), 
                                            'owner_p2p_port': info.get('owner_p2p_port', 'N/A')
                                        } 
                                        for name, info in registered_channels.items()
                                    }
                            send_message_to_peer(peer_id, {'type': 'channel_list', 'channels': channels_payload})

                        # --- Handle Unknown Login Type ---
                        else:
                             send_error_to_peer(temp_peer_id, f"Unknown login type: {msg_type}"); time.sleep(0.1); return

                    except json.JSONDecodeError:
                        print(f"[ERROR] Invalid JSON during login from {client_address}. Disconnecting.")
                        return
                    except (socket.error, OSError) as e:
                        print(f"[ERROR] Socket error during login processing: {e}")
                        return
                    except Exception as e:
                        print(f"[ERROR] Unexpected error processing login: {e}. Disconnecting.")
                        return
                else:
                    pass # Continue loop to wait for more data
            except socket.timeout:
                print(f"[TIMEOUT] Peer {client_address} timed out waiting for login info.")
                return
            except (socket.error, ConnectionResetError, BrokenPipeError) as e:
                print(f"[ERROR] Socket error receiving login info: {e}. Disconnecting.")
                return
            except UnicodeDecodeError:
                print(f"[ERROR] Non-UTF8 data during login from {client_address}. Disconnecting.")
                return

        # --- Check if Registration Succeeded ---
        if not registration_success:
            print(f"[INFO] Registration failed or not completed for {client_address}.")
            return # Exit handler thread

        # --- POST-REGISTRATION MESSAGE HANDLING PHASE ---
        print(f"[INFO] Peer '{registered_peer_name}' ({peer_id}) now in message handling phase.")
        client_socket.settimeout(None) # Disable timeout for ongoing comms
        buffer = "" # Reset buffer

        while not shutdown_requested:
            try:
                data = client_socket.recv(BUFFER_SIZE)
                if not data:
                    print(f"[DISCONNECTED] Peer '{registered_peer_name}' ({peer_id}) disconnected gracefully.")
                    break # Exit message loop

                buffer += data.decode('utf-8')
                while '\n' in buffer:
                    message_json, buffer = buffer.split('\n', 1)
                    if not message_json.strip(): continue

                    try:
                        message = json.loads(message_json)
                        msg_type = message.get('type')

                        # --- Handle 'create_channel' Request ---
                        if msg_type == 'create_channel':
                            # Add check: maybe only logged-in users can create channels?
                            if is_client_guest:
                                send_error_to_peer(peer_id, "Guests cannot create channels.")
                                continue

                            channel_name = message.get('channel_name')
                            channel_type = message.get('channel_type')
                            owner_name_from_msg = message.get('owner_name')
                            # Validation... (same as your previous code, but use peer_id)
                            if not channel_name or not owner_name_from_msg: send_error_to_peer(peer_id, "Invalid create_channel format."); continue
                            if owner_name_from_msg != registered_peer_name: send_error_to_peer(peer_id, "Permission denied: owner mismatch."); continue
                            clean_channel_name = str(channel_name).strip();
                            if not clean_channel_name or len(clean_channel_name) > 50 or ' ' in clean_channel_name: send_error_to_peer(peer_id, f"Invalid channel name."); continue

                            # Channel creation logic... (same as your previous code)
                            channel_created_successfully = False
                            with channel_lock:
                                if clean_channel_name in registered_channels: send_error_to_peer(peer_id, f"Channel name '{clean_channel_name}' taken.")
                                else:
                                    new_channel_info = {'channel_type': channel_type, 'owner_id': peer_id, 'owner_name': registered_peer_name, 'owner_ip': client_ip, 'owner_p2p_port': peer_info['p2p_port'], 'created_at': time.time()}
                                    registered_channels[clean_channel_name] = new_channel_info
                                    channel_created_successfully = True
                                    print(f"[REGISTRY] Registered channel '{clean_channel_name}' by {registered_peer_name}.")

                            if channel_created_successfully:
                                send_message_to_peer(peer_id, {'type': 'channel_created','channel_name': clean_channel_name,'owner_name': registered_peer_name})
                            #      broadcast_update({'type': 'new_channel_available','channel_name': clean_channel_name,'owner_name': registered_peer_name,'owner_ip': client_ip,'owner_p2p_port': peer_info['p2p_port']}, exclude_peer_id=peer_id)

                        # --- Handle 'request_channel_list' Request ---
                        elif msg_type == 'request_channel_list':
                            print(f"[REQUEST] Peer '{registered_peer_name}' requested channel list.")
                            channels_payload = {}
                            with channel_lock:
                                if is_client_guest:
                                    channels_payload = { 
                                        name: {
                                            'owner_name': info.get('owner_name', 'N/A'), 
                                            'owner_ip': info.get('owner_ip', 'N/A'), 
                                            'owner_p2p_port': info.get('owner_p2p_port', 'N/A')
                                        } 
                                        for name, info in registered_channels.items()
                                        if info.get('channel_type') == 'public'
                                    }
                                else:
                                    channels_payload = { 
                                        name: {
                                            'owner_name': info.get('owner_name', 'N/A'), 
                                            'owner_ip': info.get('owner_ip', 'N/A'), 
                                            'owner_p2p_port': info.get('owner_p2p_port', 'N/A')
                                        } 
                                        for name, info in registered_channels.items()
                                    }
                            send_message_to_peer(peer_id, {'type': 'channel_list', 'channels': channels_payload})

                        # --- Handle 'update_visibility' Request ---
                        elif msg_type == 'update_visibility':
                            new_visibility = message.get('is_invisible', False)
                            print(f"[VISIBILITY] Peer '{registered_peer_name}' changed visibility to: {'invisible' if new_visibility else 'visible'}")
                            # Update visibility status in the appropriate peer dictionary
                            with peer_lock:
                                if is_client_guest:
                                    if peer_id in guest_peers:
                                        guest_peers[peer_id]['is_invisible'] = new_visibility
                                else:
                                    if peer_id in logged_in_peers:
                                        logged_in_peers[peer_id]['is_invisible'] = new_visibility
                            
                            # Send updated peer list to all peers
                            current_peers_dict = get_peers_dict()
                            for pid in current_peers_dict:
                                if pid != peer_id:  # Don't send to self
                                    peers_for_client = {
                                        p: info 
                                        for p, info in current_peers_dict.items() 
                                        if p != pid and not info.get('is_invisible', False)
                                    }
                                    send_message_to_peer(pid, {'type': 'peer_list', 'peers': peers_for_client})

                        # --- Handle other message types ---
                        # Add handlers for /join, /leave, /ch_msg etc. later
                        else:
                            print(f"[WARNING] Unknown message type '{msg_type}' from '{registered_peer_name}' ({peer_id}): {message_json}")

                    except json.JSONDecodeError: 
                        print(f"[ERROR] Invalid JSON from '{registered_peer_name}': {message_json}")
                    except UnicodeDecodeError: 
                        print(f"[ERROR] Corrupted UTF-8 from '{registered_peer_name}'.")
                        buffer = "" # Clear buffer on decode error
                    except (socket.error, OSError) as send_err: 
                        print(f"[ERROR] Socket error processing '{registered_peer_name}': {send_err}")
                        raise send_err # Raise to outer handler
                    except Exception as e: 
                        print(f"[ERROR] Processing message from '{registered_peer_name}': {e} - Msg: {message_json}")

            except (ConnectionResetError, BrokenPipeError, socket.error, OSError) as e:
                if not shutdown_requested: 
                    print(f"[DISCONNECTED] Peer '{registered_peer_name}' ({peer_id}) connection error: {e}")
                break
            except UnicodeDecodeError: 
                print(f"[ERROR] Non-UTF8 stream from '{registered_peer_name}'. Closing.")
                break
            except Exception as e:
                if not shutdown_requested: 
                    print(f"[ERROR] Unexpected error handling '{registered_peer_name}': {e}")
                break

    except Exception as e:
        if not shutdown_requested:
            peer_desc = f"'{registered_peer_name}' ({peer_id})" if registered_peer_name else f"{client_ip}:{client_registry_port}"
            print(f"[CRITICAL ERROR] Unhandled exception in handle_client for {peer_desc}: {e}")
            import traceback
            traceback.print_exc()

    # --- CLEANUP PHASE ---
    finally:
        peer_desc = f"'{registered_peer_name}' ({peer_id})" if registered_peer_name else f"{client_ip}:{client_registry_port} (Unregistered)"
        print(f"[CLEANUP] Closing connection for {peer_desc}.")

        removed_from_registry = False
        broadcast_leave = False

        if peer_id: # Only modify registry if peer was successfully registered
            with peer_lock:
                # Remove from the correct dictionary
                if is_client_guest:
                    if peer_id in guest_peers and guest_peers[peer_id]['socket'] == client_socket:
                        del guest_peers[peer_id]
                        removed_from_registry = True
                        print(f"[REMOVED GUEST] {peer_desc}. Guests remaining: {len(guest_peers)}")
                else: # Logged-in user
                    if peer_id in logged_in_peers and logged_in_peers[peer_id]['socket'] == client_socket:
                        del logged_in_peers[peer_id]
                        removed_from_registry = True
                        broadcast_leave = True # Only broadcast leaves for logged-in users
                        print(f"[REMOVED USER] {peer_desc}. Logged-in peers remaining: {len(logged_in_peers)}")

            # Clean up channels owned by this user (if they were logged in)
            # Check if this user owned any channels and remove them or mark as orphaned?
            # Simpler: Channels persist for now even if owner leaves. Re-login can reclaim? Or manual cleanup.
            # More complex: Transfer ownership? Delete channels?
            # For now: Channels remain.
            # if removed_from_registry and not is_client_guest:
            #    handle_channel_owner_disconnect(peer_id)

        else:
            print(f"[CLEANUP] Unregistered peer {client_address} connection closed.")

        # Close socket robustly
        if client_socket:
            try: client_socket.shutdown(socket.SHUT_RDWR)
            except: pass
            try: client_socket.close()
            except: pass

        # Broadcast leave *only* for logged-in users removed by this thread
        if broadcast_leave and removed_from_registry and peer_id and not shutdown_requested:
            print(f"[BROADCAST] Notifying logged-in peers about {registered_peer_name} leaving.")
            # Send to other logged-in users
            broadcast_update({'type': 'peer_left', 'id': peer_id, 'name': registered_peer_name}, exclude_peer_id=peer_id)

        print(f"[CLEANUP] Thread finished for {peer_desc}.")


# --- Main Server Loop (Mostly unchanged) ---
def start_registry_server():
    global shutdown_requested
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server_socket.bind((HOST, PORT))
        server_socket.listen(10)
        print(f"[SERVER START] Registry server listening on {HOST}:{PORT}")
        server_socket.settimeout(1.0) # Timeout for accept

        while not shutdown_requested:
            try:
                client_socket, client_address = server_socket.accept()
                # Start a new thread for each client
                thread = threading.Thread(target=handle_client, args=(client_socket, client_address), daemon=True)
                thread.start()
            except socket.timeout:
                continue # Just check shutdown_requested flag again
            except OSError as e: # Handle socket closed during accept
                 if not shutdown_requested: print(f"[SERVER ERROR] Accept error: {e}")
                 break # Exit loop if socket is closed unexpectedly

    except OSError as e: print(f"[SERVER CRITICAL] Cannot bind to {HOST}:{PORT}: {e}")
    except KeyboardInterrupt: print("\n[SERVER STOP] Shutdown requested via KeyboardInterrupt.")
    finally:
        shutdown_requested = True
        print("[SERVER SHUTDOWN] Closing server socket...")
        server_socket.close()
        # Wait briefly for client handler threads to notice shutdown_requested?
        print("[SERVER SHUTDOWN] Notifying connected peers...")
        # Implement logic to send a 'server_shutdown' message if desired
        # ...
        print("[SERVER SHUTDOWN] Server stopped.")

# --- Entry Point ---
if __name__ == "__main__":
    start_registry_server()