# --- p2p_client.py ---
import socket
import threading
import sys
import time
import json
import random
import ipaddress

# --- Configuration (Keep as before) ---
REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 9999
LISTEN_HOST = '0.0.0.0'
MY_P2P_PORT = random.randint(10000, 60000)
BUFFER_SIZE = 4096

# --- Shared State (Keep as before, MY_NAME/MY_ID set later) ---
peer_list_lock = threading.Lock()
known_peers = {} # Stores logged-in peers received from server
my_channels_lock = threading.Lock()
my_channels = {} # Channels client owns or is in
running = True
MY_NAME = None
MY_ID = None
is_guest = None
MY_INFO = {} # Populated after P2P listener starts and potentially corrected by server

# --- Helper Functions (Keep get_local_ip, update_my_info, get_peer_name_by_address, get_peer_info_by_name) ---
# ... (Include the exact functions from your previous code) ...
def get_local_ip():
    """Try to get a non-loopback local IP address."""
    # ... (implementation as before) ...
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(0.1); s.connect(('8.8.8.8', 80)); ip = s.getsockname()[0]; s.close(); return ip
    except Exception:
        try: return socket.gethostbyname(socket.gethostname())
        except socket.gaierror: return '127.0.0.1'

def update_my_info(name, p2p_port, ip=None):
    """Updates MY_INFO with determined or server-provided IP."""
    global MY_INFO
    current_ip = ip
    if current_ip is None:
        current_ip = get_local_ip()
        # Optional: Add warning if loopback when LISTEN_HOST is 0.0.0.0
        # if LISTEN_HOST == '0.0.0.0' and ipaddress.ip_address(current_ip).is_loopback:
        #    print(f"[WARNING] Determined local IP {current_ip} is loopback...")

    new_info = {'ip': current_ip, 'p2p_port': p2p_port, 'name': name}
    if MY_INFO != new_info: # Only print if changed
        MY_INFO = new_info
        source = "(from server)" if ip else "(determined)"
        print(f"[INFO] Updated own info {source}: {MY_INFO}")

def get_peer_name_by_address(ip, port):
    # ... (implementation as before) ...
    if MY_INFO.get('ip') == ip and MY_INFO.get('p2p_port') == port: return MY_NAME + " (Self)"
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('ip') == ip and info.get('p2p_port') == port: return info.get('name', f"{ip}:{port}")
    return f"{ip}:{port}"

def get_peer_info_by_name(name):
    # ... (implementation as before) ...
    if name == MY_NAME: return MY_INFO
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('name') == name: return info
    return None
# --- P2P Listener (Keep handle_peer_connection, listen_for_peers) ---
# ... (Include the exact functions from your previous code) ...
def handle_peer_connection(peer_socket, peer_address):
    # ... (implementation as before) ...
    peer_name = get_peer_name_by_address(peer_address[0], peer_address[1])
    print(f"\n[P2P <<] Incoming connection from {peer_address[0]}:{peer_address[1]} ({peer_name})")
    try:
        while True:
            data = peer_socket.recv(BUFFER_SIZE);
            if not data: print(f"\n[P2P <<] Peer {peer_name} disconnected."); break
            message = data.decode('utf-8')
            print(f"\r{' ' * 60}\r[P2P MSG from {peer_name}]: {message}\n> ", end="", flush=True)
    except ConnectionResetError: print(f"\n[P2P <<] Peer {peer_name} connection reset.")
    except Exception as e: print(f"\n[P2P ERROR] Error with peer {peer_name}: {e}")
    finally: peer_socket.close()

def listen_for_peers(host, port):
    """Starts P2P listener thread."""
    # ... (implementation as before, ensure it uses global `running`) ...
    p2p_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    p2p_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        p2p_server_socket.bind((host, port))
        p2p_server_socket.listen(5)
        print(f"[P2P LISTENING] Ready for P2P connections on {host}:{port}")
        global running
        while running:
            try:
                p2p_server_socket.settimeout(1.0)
                peer_socket, peer_address = p2p_server_socket.accept()
                p2p_server_socket.settimeout(None)
                peer_thread = threading.Thread(target=handle_peer_connection, args=(peer_socket, peer_address), daemon=True)
                peer_thread.start()
            except socket.timeout: continue
            except socket.error as e:
                if running: print(f"[P2P LISTENER ERROR] Socket error: {e}"); break
            except Exception as e: print(f"[P2P LISTENER CRITICAL ERROR] Unexpected error: {e}"); break
    except OSError as e:
        print(f"[P2P LISTENER BIND ERROR] Could not bind P2P listener to {host}:{port} - {e}.")
        running = False # Signal failure
    finally:
        print("[P2P LISTENING] Shutting down P2P listener.")
        p2p_server_socket.close()

# --- Registry Listener (Handles messages *after* initial login) ---
def listen_to_registry(registry_socket):
    """Listens for updates from the registry *after* successful registration."""
    global known_peers, running, MY_INFO, my_channels # MY_NAME/ID already set
    buffer = ""
    prompt = f"{MY_NAME}> "
    while running:
        try:
            data = registry_socket.recv(BUFFER_SIZE)
            if not data:
                if running: print("\n[CONNECTION LOST] Registry server closed the connection.")
                running = False
                break
            buffer += data.decode('utf-8')

            while '\n' in buffer:
                message_json, buffer = buffer.split('\n', 1)
                if not message_json.strip(): continue

                try:
                    message = json.loads(message_json)
                    msg_type = message.get('type')
                    # print(f"[DEBUG RECV] {message_json}") # Debugging

                    # NOTE: No 'request_p2p_port' or 'registration_ack' expected here anymore.
                    # Initial ack is handled in `login_or_guest`.
                    # Server might send updated ack later if IP changes etc, handle if needed.

                    if msg_type == 'peer_list':
                        # ... (Keep peer list handling as before) ...
                        print("\n[REGISTRY] Received peer list update (logged-in users).")
                        server_peers = message.get('peers', {})
                        with peer_list_lock:
                            known_peers.clear()
                            # Filter out self if server includes it
                            known_peers = {pid: info for pid, info in server_peers.items() if info.get('name') != MY_NAME}
                            # Server *should* have already validated our info during login_ack
                            # Re-check MY_INFO if server includes self in list? Generally not needed now.

                    elif msg_type == 'peer_joined':
                         # ... (Keep peer joined handling as before, check name != MY_NAME) ...
                         peer_id = message.get('id'); peer_info = message.get('peer_info'); peer_name = peer_info.get('name', 'Unk') if peer_info else 'Unk'
                         if peer_id and peer_info and peer_name != MY_NAME:
                             join_type = "Peer" # Assume logged-in unless server specifies guest type
                             # if peer_info.get('guest'): join_type = "Guest" # If server adds distinction
                             with peer_list_lock: known_peers[peer_id] = peer_info # Add to known (logged-in) peers
                             print(f"\r{' ' * 60}\r[REGISTRY] {join_type} joined: {peer_name} ({peer_info.get('ip')}:{peer_info.get('p2p_port')})\n{prompt}", end="", flush=True)


                    elif msg_type == 'peer_left':
                        # ... (Keep peer left handling as before) ...
                        peer_id = message.get('id')
                        if peer_id:
                             removed_name = "Unknown"
                             with peer_list_lock:
                                 if peer_id in known_peers: removed_name = known_peers.pop(peer_id).get('name', 'Unk')
                             if removed_name != "Unknown": # Only print if we knew them
                                print(f"\r{' ' * 60}\r[REGISTRY] Peer left: {removed_name}\n{prompt}", end="", flush=True)

                    # --- Channel Message Handling (Keep as before) ---
                    elif msg_type == 'channel_created':
                         # ... (handle channel created confirmation) ...
                         c_name = message.get('channel_name'); c_owner = message.get('owner_name')
                         print(f"\r{' ' * 60}\r[REGISTRY] Channel '{c_name}' creation confirmed.\n{prompt}", end="", flush=True)
                         if c_owner == MY_NAME:
                             with my_channels_lock:
                                 if c_name not in my_channels:
                                     # Make sure MY_INFO is populated
                                     if not MY_INFO: update_my_info(MY_NAME, MY_P2P_PORT)
                                     my_channels[c_name] = {'owner': MY_NAME, 'members': {MY_NAME: MY_INFO.copy()}}
                                     if not MY_INFO: print(f"[WARN] MY_INFO empty for channel {c_name}")

                    elif msg_type == 'channel_error':
                        # ... (handle channel error message) ...
                        error_msg = message.get('message', 'Unknown channel error'); c_name = message.get('channel_name', 'N/A')
                        print(f"\r{' ' * 60}\r[REGISTRY ERROR] Channel '{c_name}': {error_msg}\n{prompt}", end="", flush=True)

                    elif msg_type == 'new_channel_available':
                        # ... (handle new channel announcement) ...
                        c_name = message.get('channel_name'); c_owner = message.get('owner_name')
                        print(f"\r{' ' * 60}\r[REGISTRY] New channel available: '{c_name}' (Owner: {c_owner})\n{prompt}", end="", flush=True)

                    elif msg_type == 'channel_list':
                        # ... (handle receiving list of all channels) ...
                        print(f"\r{' ' * 60}\r[REGISTRY] Received list of all channels:")
                        channels = message.get('channels', {})
                        if channels:
                            for name in sorted(channels.keys()):
                                info = channels[name]; owner = info.get('owner_name','N/A'); contact = f"{info.get('owner_ip','N/A')}:{info.get('owner_p2p_port','N/A')}"
                                print(f"  - {name} (Owner: {owner} @ {contact})")
                        else: print("  (No channels currently registered)")
                        print(f"{prompt}", end="", flush=True)

                    elif msg_type == 'error': # Generic error from server after registration
                        error_msg = message.get('message', 'Unknown server error')
                        print(f"\r{' ' * 60}\r[SERVER ERROR] {error_msg}\n{prompt}", end="", flush=True)

                    else:
                        print(f"\r{' ' * 60}\r[REGISTRY MSG UNHANDLED] Type: {msg_type}\n{prompt}", end="", flush=True)

                except json.JSONDecodeError: print(f"\r{' ' * 60}\r[REGISTRY ERROR] Invalid JSON: {message_json}\n{prompt}", end="", flush=True)
                except socket.error as e:
                    if running: print(f"\n[ERROR] Socket error in registry listener: {e}"); running = False; break
                except Exception as e: print(f"\n[ERROR] Processing registry message: {e}\nMessage: {message_json}")

        except (ConnectionResetError, ConnectionAbortedError):
            if running: print("\n[CONNECTION LOST] Connection to registry closed.")
            running = False
        except OSError as e:
            if running: print(f"\n[SOCKET ERROR] Registry listener: {e}")
            running = False
        except Exception as e:
            if running: print(f"\n[ERROR] Receiving from registry: {e}")
            running = False

    print("[THREAD] Registry listener thread finished.")
    running = False # Ensure main loop stops

# --- Function to send P2P message (Keep as before) ---
# ... (Include the exact function from your previous code) ...
def send_p2p_message(target_name, message):
    # ... (implementation as before) ...
    target_info = get_peer_info_by_name(target_name)
    if not target_info: print(f"[P2P ERROR] Peer '{target_name}' not found."); return False
    if not target_info.get('ip') or not target_info.get('p2p_port'): print(f"[P2P ERROR] Peer '{target_name}' incomplete info."); return False
    target_ip = target_info['ip']; target_port = target_info['p2p_port']
    # print(f"[P2P >>] Attempting to send to {target_name} ({target_ip}:{target_port})...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_socket:
             peer_socket.settimeout(5.0); peer_socket.connect((target_ip, target_port))
             peer_socket.sendall(message.encode('utf-8')); return True
    except socket.timeout: print(f"[P2P ERROR] Connection to {target_name} timed out.")
    except ConnectionRefusedError: print(f"[P2P ERROR] Connection to {target_name} refused.")
    except OSError as e: print(f"[P2P ERROR] Network error sending to {target_name}: {e}")
    except Exception as e: print(f"[P2P ERROR] Unexpected error sending to {target_name}: {e}")
    return False

# --- Function to send Create Channel Request (Keep as before) ---
def create_channel(registry_socket, channel_name):
    """Sends a request to the registry server to create a new channel."""
    # ... (implementation as before, uses MY_NAME, MY_INFO) ...
    if not MY_NAME: print("[ERROR] Cannot create channel: Name not set."); return False
    if not MY_INFO: print("[ERROR] Cannot create channel: Own info not known."); return False
    try:
        create_msg_dict = {'type': 'create_channel','channel_name': channel_name,'owner_name': MY_NAME,'owner_ip': MY_INFO.get('ip'),'owner_p2p_port': MY_INFO.get('p2p_port')}
        create_msg_json = json.dumps(create_msg_dict) + "\n"
        # print(f"[CMD] Sending channel creation request: {create_msg_json.strip()}")
        registry_socket.sendall(create_msg_json.encode('utf-8'))
        print(f"[CMD] Request sent for channel '{channel_name}'. Waiting for confirmation...")
        return True
    except socket.error as e: print(f"[ERROR] Socket error sending create channel request: {e}"); return False
    except Exception as e: print(f"[ERROR] Unexpected error sending create channel request: {e}"); return False

# --- NEW: Initial Login/Guest Function ---
def login_or_guest(host, port):
    """
    Handles the initial user command (/guest or /login), connects to the
    registry, sends the initial identification message, and waits for ack.

    Returns:
        tuple: (socket object, bool success) or (None, False) on failure/exit.
    """
    global MY_NAME, MY_ID, is_guest, MY_INFO

    registry_socket = None # Initialize socket variable

    while True: # Loop for user command input
        try:
            print("\n--- Welcome to P2P Client ---")
            print("Identify yourself to connect:")
            print("  /guest <your_desired_name>")
            print("  /login <your_login_name> <your_numeric_ID>")
            print("  (Press Ctrl+C or Ctrl+D to exit)")
            prompt = "> "
            cmd = input(prompt).strip()

            if not cmd: continue

            cmd_lower = cmd.lower()
            login_data = None # Will hold the dict to send

            # --- Parse /guest ---
            if cmd_lower.startswith('/guest '):
                parts = cmd.split(' ', 1)
                if len(parts) == 2 and parts[1].strip():
                    temp_name = parts[1].strip()
                    if len(temp_name) > 0 and len(temp_name) <= 50 and ' ' not in temp_name: # Basic validation
                        MY_NAME = temp_name
                        MY_ID = None
                        is_guest = True
                        update_my_info(MY_NAME, MY_P2P_PORT) # Determine IP/Port *before* sending
                        login_data = {
                            'type': 'guest_login',
                            'name': MY_NAME,
                            'ip': MY_INFO.get('ip'),      # Send determined IP
                            'p2p_port': MY_P2P_PORT     # Send our listening port
                        }
                        print(f"[INFO] Attempting login as Guest: {MY_NAME}")
                    else:
                        print("[ERROR] Invalid guest name (1-50 chars, no spaces).")
                else:
                    print("[ERROR] Invalid format. Use: /guest <name>")

            # --- Parse /login ---
            elif cmd_lower.startswith('/login '):
                parts = cmd.split(' ', 2)
                if len(parts) == 3:
                    temp_name = parts[1].strip()
                    temp_id = parts[2].strip()
                    if not temp_name or len(temp_name) > 50 or ' ' in temp_name:
                        print("[ERROR] Invalid login name (1-50 chars, no spaces).")
                    elif not temp_id.isdigit():
                        print("[ERROR] Login ID must be numeric.")
                    else:
                        MY_NAME = temp_name
                        MY_ID = temp_id
                        is_guest = False
                        update_my_info(MY_NAME, MY_P2P_PORT) # Determine IP/Port *before* sending
                        login_data = {
                            'type': 'user_login',
                            'name': MY_NAME,
                            'id': MY_ID,
                            'ip': MY_INFO.get('ip'),      # Send determined IP
                            'p2p_port': MY_P2P_PORT     # Send our listening port
                        }
                        print(f"[INFO] Attempting login as User: {MY_NAME} (ID: {MY_ID})")
                else:
                    print("[ERROR] Invalid format. Use: /login <name> <numeric_ID>")

            # --- Handle unknown commands ---
            else:
                print(f"[ERROR] Unknown command or invalid format.")


            # --- If login_data is set, attempt connection and send ---
            if login_data:
                try:
                    print(f"[CONNECTING] Connecting to Registry Server {host}:{port}...")
                    registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    registry_socket.settimeout(10.0) # Connection timeout
                    registry_socket.connect((host, port))
                    print("[CONNECTED] Connected. Sending identification...")

                    # Send the login/guest message
                    login_msg_json = json.dumps(login_data) + "\n"
                    registry_socket.sendall(login_msg_json.encode('utf-8'))

                    # --- Wait for Acknowledgement or Error ---
                    print("[WAITING] Waiting for server acknowledgement...")
                    buffer = ""
                    ack_received = False
                    success = False
                    while not ack_received:
                        try:
                            # Use a reasonable timeout for the ack
                            # registry_socket.settimeout(20.0) # Already set above
                            data = registry_socket.recv(BUFFER_SIZE)
                            if not data:
                                print("[ERROR] Connection closed by server before acknowledgement.")
                                ack_received = True # Exit loop
                                success = False
                                break # Break inner recv loop
                            buffer += data.decode('utf-8')
                            if '\n' in buffer:
                                response_json, buffer = buffer.split('\n', 1)
                                if not response_json.strip(): continue

                                # print(f"[DEBUG ACK RECV] {response_json}") # Debugging
                                response = json.loads(response_json)
                                resp_type = response.get('type')

                                if resp_type == 'login_ack' or resp_type == 'guest_ack':
                                    print(f"[SUCCESS] Server acknowledged registration: {response.get('message')}")
                                    # Update IP if server provided a corrected one
                                    server_ip = response.get('client_ip')
                                    if server_ip:
                                        update_my_info(MY_NAME, MY_P2P_PORT, ip=server_ip)
                                    ack_received = True
                                    success = True
                                    # Keep the socket timeout reasonable for normal comms or set to None
                                    registry_socket.settimeout(None) # Or a long timeout
                                    return registry_socket, True # Return socket and success

                                elif resp_type == 'error':
                                    print(f"[FAILED] Server rejected registration: {response.get('message')}")
                                    ack_received = True
                                    success = False
                                    # Close the socket on error before returning
                                    try: registry_socket.close()
                                    except: pass
                                    registry_socket = None
                                    # Loop back to command prompt
                                    break # Break inner recv loop, will continue outer command loop
                                else:
                                    print(f"[WARNING] Unexpected response type during ack: {resp_type}")
                                    # Decide whether to continue waiting or fail

                        except socket.timeout:
                            print("[ERROR] Timeout waiting for server acknowledgement.")
                            ack_received = True; success = False; break
                        except (socket.error, ConnectionResetError, json.JSONDecodeError, UnicodeDecodeError) as e:
                            print(f"[ERROR] Error receiving/parsing acknowledgement: {e}")
                            ack_received = True; success = False; break

                    # If we broke from the ack wait loop without success
                    if not success:
                        if registry_socket:
                             try: registry_socket.close()
                             except: pass
                             registry_socket = None
                        print("[INFO] Please try logging in or connecting as guest again.")
                        continue # Continue the outer command loop

                except socket.timeout:
                    print(f"[ERROR] Connection to {host}:{port} timed out.")
                    if registry_socket: registry_socket.close(); registry_socket = None
                    # Optionally exit or retry after a delay
                    print("[INFO] Please check server status and try again.")
                    time.sleep(2); continue # Continue outer loop
                except socket.error as e:
                    print(f"[ERROR] Could not connect to registry server at {host}:{port}: {e}")
                    if registry_socket: registry_socket.close(); registry_socket = None
                    print("[INFO] Please check server status and try again.")
                    time.sleep(2); continue # Continue outer loop
                except Exception as e:
                    print(f"[CRITICAL] Unexpected error during login attempt: {e}")
                    if registry_socket: registry_socket.close(); registry_socket = None
                    return None, False # Exit client

            # If login_data wasn't set (invalid command), loop continues automatically

        except (EOFError, KeyboardInterrupt):
            print("\n[EXITING] Client startup interrupted.")
            if registry_socket: registry_socket.close()
            return None, False # Exit client

# --- Main Client Function (Modified Flow) ---
def start_p2p_client():
    global running, MY_NAME, MY_ID, is_guest, MY_INFO, my_channels, known_peers

    # 1. Start P2P listener first (needs MY_P2P_PORT defined globally)
    print(f"[INFO] Starting P2P listener on {LISTEN_HOST}:{MY_P2P_PORT}...")
    p2p_listener_thread = threading.Thread(target=listen_for_peers, args=(LISTEN_HOST, MY_P2P_PORT), daemon=True)
    p2p_listener_thread.start()
    time.sleep(0.3) # Give listener a moment

    # Check if P2P listener failed immediately (e.g., port busy)
    if not running:
        print("[CRITICAL] P2P listener failed to start. Exiting.")
        return

    # 2. Handle initial Login/Guest identification and connection
    # This function now handles connecting and waiting for ack
    registry_socket, login_success = login_or_guest(REGISTRY_HOST, REGISTRY_PORT)

    if not login_success:
        print("[EXITING] Failed to login or register as guest.")
        running = False # Signal threads to stop
        # Wait briefly for P2P listener to potentially stop
        time.sleep(0.5)
        return # Exit client application

    # --- If login/guest was successful ---
    print("-" * 30)
    if is_guest: print(f"--- Welcome, Guest {MY_NAME} ---")
    else: print(f"--- Welcome, {MY_NAME} (ID: {MY_ID}) ---")
    print(f"Your Info: {MY_INFO}")
    print("--- Available Commands ---")
    print("  /list              - Show known peers (logged-in users)")
    print("  /myinfo            - Show your info")
    print("  /msg <name> <msg>  - Send direct message")
    print("  /create <channel>  - Create a new channel")
    print("  /list_channels     - Request list of all channels")
    print("  /my_channels       - List channels you are in")
    print("  /members <channel> - List members of owned channel")
    print("  /quit              - Exit")
    print("-" * 30)

    # 3. Start thread to listen for ongoing Registry updates
    registry_listener_thread = threading.Thread(target=listen_to_registry, args=(registry_socket,), daemon=True)
    registry_listener_thread.start()

    # 4. Main loop for user commands (post-registration)
    while running:
        try:
             prompt = f"{MY_NAME}> "
             cmd = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
             print("\n[INFO] Quit signal received.")
             cmd = "/quit"

        if not running: break
        if not cmd: continue

        cmd_lower = cmd.lower()

        if cmd_lower == '/quit':
            running = False; break

        # --- Process other commands using registry_socket ---
        elif cmd_lower == '/list':
             # ... (Keep implementation as before) ...
            print("[KNOWN PEERS (Logged-in)]:")
            with peer_list_lock:
                if known_peers:
                     count = 0
                     for info in sorted(known_peers.values(), key=lambda x: x.get('name', 'zzzz')):
                         # No need to check name != MY_NAME if server excludes self
                         print(f"  - {info.get('name', 'Unk')} ({info.get('ip', 'N/A')}:{info.get('p2p_port', 'N/A')})"); count += 1
                     if count == 0: print("  (No other peers known)")
                else: print("  (List empty or not received)")

        elif cmd_lower == '/myinfo':
             # ... (Keep implementation as before) ...
            print("[YOUR INFO]:"); print(f"  Data: {MY_INFO}"); print(f"  Status: {'Guest' if is_guest else f'User (ID: {MY_ID})'}")

        elif cmd_lower.startswith('/msg '):
             # ... (Keep implementation as before) ...
             parts = cmd.split(' ', 2);
             if len(parts)==3:
                 if parts[1] == MY_NAME: print("[CMD ERROR] Cannot /msg yourself.")
                 else: send_p2p_message(parts[1], parts[2])
             else: print("[CMD ERROR] Usage: /msg <name> <message>")

        elif cmd_lower.startswith('/create '):
             # ... (Keep implementation as before, uses registry_socket) ...
             parts = cmd.split(' ', 1);
             if len(parts)==2 and parts[1].strip(): create_channel(registry_socket, parts[1].strip())
             else: print("[CMD ERROR] Usage: /create <channel_name>")

        elif cmd_lower == '/list_channels':
             # ... (Keep implementation as before, sends request via registry_socket) ...
             print("[CMD] Requesting channel list from server...")
             try:
                 request_msg = json.dumps({'type': 'request_channel_list'}) + "\n"
                 registry_socket.sendall(request_msg.encode('utf-8'))
             except socket.error as e: print(f"[ERROR] Failed to send request: {e}")
             except Exception as e: print(f"[ERROR] Unexpected error sending request: {e}")

        elif cmd_lower == '/my_channels':
             # ... (Keep implementation as before) ...
             print("[YOUR CHANNELS]:");
             with my_channels_lock:
                 if my_channels:
                     for name, data in sorted(my_channels.items()):
                         role = "Owner" if data.get('owner') == MY_NAME else "Member"; count = len(data.get('members', {})) if role=="Owner" else "?"; owner=data.get('owner','N/A')
                         print(f"  - {name} ({role}, Owner: {owner}, {count} member(s))")
                 else: print("  (Not in any channels)")

        elif cmd_lower.startswith('/members '):
             # ... (Keep implementation as before) ...
             parts = cmd.split(' ', 1);
             if len(parts)==2 and parts[1].strip():
                 channel_name = parts[1].strip()
                 with my_channels_lock:
                     if channel_name in my_channels:
                         channel_data = my_channels[channel_name]
                         if channel_data.get('owner') == MY_NAME:
                             print(f"[MEMBERS of '{channel_name}']: (You are Owner)"); members = channel_data.get('members',{})
                             if members:
                                 for name in sorted(members.keys()): print(f"  - {name} {'(You)' if name == MY_NAME else ''}")
                             else: print("  (Error: Member list empty?)")
                         else: print(f"[INFO] You are member of '{channel_name}'. Owner ({channel_data.get('owner','Unk')}) manages list.")
                     else: print(f"[CMD ERROR] Not part of channel '{channel_name}'.")
             else: print("[CMD ERROR] Usage: /members <channel_name>")


        else: print("[CMD ERROR] Unknown command.")

    # --- Cleanup --- (Remains mostly the same)
    if running: print("\n[EXITING] Shutting down...") # Should be false now
    running = False # Ensure flag is false

    print("[CLOSING] Closing connection to registry...")
    try:
        if registry_socket:
             registry_socket.shutdown(socket.SHUT_RDWR)
             registry_socket.close()
    except (socket.error, OSError, Exception): pass

    print("[INFO] Waiting for background threads...")
    time.sleep(0.5)
    print("[CLOSED] Client shut down.")


# --- Entry Point ---
if __name__ == "__main__":
    start_p2p_client()