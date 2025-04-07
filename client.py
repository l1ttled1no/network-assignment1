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

# --- Shared State ---
peer_list_lock = threading.Lock()
known_peers = {} # Stores logged-in peers received from server

my_channels_lock = threading.Lock()
# Channels client owns or is in
# Format: { channel_name: {'owner': owner_name, 'members': {member_name: {'ip': ip, 'p2p_port': port}}} }
my_channels = {}

# NEW: Dictionary to store info about all available channels from the server
# Format: { channel_name: {'owner_name': name, 'owner_ip': ip, 'owner_p2p_port': port} }
available_channels_info_lock = threading.Lock()
available_channels_info = {}

running = True
MY_NAME = None
MY_ID = None # For user login
is_guest = None # Distinguishes guest/user
MY_INFO = {} # Populated after P2P listener starts
is_invisible = False # NEW: Track if user is invisible
is_offline = False # NEW: Track if user is offline

# --- Helper Functions (Keep get_local_ip, update_my_info, get_peer_name_by_address, get_peer_info_by_name) ---
def get_local_ip():
    """Try to get a non-loopback local IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(0.1); s.connect(('8.8.8.8', 80)); ip = s.getsockname()[0]; s.close(); return ip
    except Exception:
        try: return socket.gethostbyname(socket.gethostname())
        except socket.gaierror: return '127.0.0.1'

def update_my_info(name, p2p_port, ip=None):
    """Updates MY_INFO with determined or server-provided IP."""
    global MY_INFO
    current_ip = ip
    if current_ip is None: current_ip = get_local_ip()
    new_info = {'ip': current_ip, 'p2p_port': p2p_port, 'name': name}
    if MY_INFO != new_info:
        MY_INFO = new_info
        source = "(from server)" if ip else "(determined)"
        print(f"[INFO] Updated own info {source}: {MY_INFO}")

def get_peer_name_by_address(ip, port):
    """Looks up peer name from known_peers or returns IP:Port."""
    if MY_INFO.get('ip') == ip and MY_INFO.get('p2p_port') == port: return MY_NAME + " (Self)"
    with peer_list_lock:
        # Check known peers (logged-in users from server list)
        for peer_id, info in known_peers.items():
            if info.get('ip') == ip and info.get('p2p_port') == port: return info.get('name', f"{ip}:{port}")
    # Fallback: Check channel members if needed (less reliable source for general lookup)
    # with my_channels_lock:
    #     for channel in my_channels.values():
    #         for member_name, member_info in channel.get('members', {}).items():
    #             if member_info.get('ip') == ip and member_info.get('p2p_port') == port:
    #                 return member_name
    return f"{ip}:{port}" # Ultimate fallback

def get_peer_info_by_name(name):
    """Looks up peer IP/Port from known_peers or local channels."""
    if name == MY_NAME: return MY_INFO
    # Prioritize known_peers list from registry (more likely up-to-date)
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('name') == name: return info
    # Fallback: Check local channel member lists
    with my_channels_lock:
        for channel in my_channels.values():
             member_info = channel.get('members', {}).get(name)
             if member_info: return member_info # Return info stored in channel member list
    return None # Not found

# --- P2P Listener ---
def handle_peer_connection(peer_socket, peer_address):
    """Handles incoming P2P connection and processes messages."""
    initial_peer_name = get_peer_name_by_address(peer_address[0], peer_address[1])
    actual_peer_name = initial_peer_name # Can be updated by message content
    # print(f"\n[P2P <<] Connection from {peer_address[0]}:{peer_address[1]} (Initial ID: {initial_peer_name})")
    prompt = f"{MY_NAME}> " # Get prompt for redraw

    try:
        buffer = ""
        while running:
            data = peer_socket.recv(BUFFER_SIZE)
            if not data:
                print(f"\n[P2P <<] Peer {actual_peer_name} ({peer_address[0]}:{peer_address[1]}) disconnected.")
                break

            buffer += data.decode('utf-8')
            while '\n' in buffer and running:
                message_json, buffer = buffer.split('\n', 1)
                if not message_json.strip(): continue

                try:
                    message = json.loads(message_json)
                    msg_type = message.get('type')

                    # --- Handle Channel Join Request ---
                    if msg_type == 'request_join_channel':
                        req_channel = message.get('channel_name')
                        req_name = message.get('requester_name')
                        req_ip = message.get('requester_ip')
                        req_port = message.get('requester_p2p_port')

                        if req_name: actual_peer_name = req_name # Update name
                        print(f"\r{' ' * 60}\r[P2P RECV] Join request for '{req_channel}' from '{req_name}'\n{prompt}", end="", flush=True)

                        if not (req_channel and req_name and req_ip and req_port):
                             print(f"[P2P WARNING] Invalid join request from {actual_peer_name}: missing fields.")
                             continue

                        with my_channels_lock:
                            if req_channel in my_channels and my_channels[req_channel].get('owner') == MY_NAME:
                                # I am the owner
                                channel_data = my_channels[req_channel]
                                members_dict = channel_data.setdefault('members', {}) # Ensure members dict exists
                                requester_info = {'name': req_name, 'ip': req_ip, 'p2p_port': req_port}

                                if req_name in members_dict:
                                     print(f"[CHANNEL INFO] '{req_name}' (already member) requested to join '{req_channel}'. Updating info & resending acceptance.")
                                     # Update info in case it changed
                                     members_dict[req_name] = requester_info
                                else:
                                     print(f"[CHANNEL] Adding '{req_name}' to your channel '{req_channel}'.")
                                     members_dict[req_name] = requester_info

                                # Send acceptance back (run send in thread to avoid blocking listener)
                                accept_response = {'type': 'join_channel_accepted','channel_name': req_channel,'owner_name': MY_NAME,'members': members_dict}
                                accept_thread = threading.Thread(target=send_p2p_message, args=(req_name, json.dumps(accept_response) + '\n'), daemon=True)
                                accept_thread.start()
                                print(f"[P2P >>] Sent join acceptance for '{req_channel}' to '{req_name}'.")

                            elif req_channel in my_channels:
                                owner = my_channels[req_channel].get('owner', 'Unk')
                                print(f"[P2P WARNING] Join request for '{req_channel}' from '{req_name}', but I'm not owner (Owner: {owner}). Ignoring.")
                            else:
                                print(f"[P2P WARNING] Join request for unknown local channel '{req_channel}' from '{req_name}'. Ignoring.")

                    # --- Handle Join Acceptance (I requested to join) ---
                    elif msg_type == 'join_channel_accepted':
                         accepted_channel = message.get('channel_name')
                         accepted_owner = message.get('owner_name')
                         accepted_members = message.get('members', {})

                         print(f"\r{' ' * 60}\r[P2P RECV] Joined channel '{accepted_channel}' (Owner: {accepted_owner})!")
                         print(f"[CHANNEL] Members: {list(accepted_members.keys())}\n{prompt}", end="", flush=True)

                         with my_channels_lock:
                             my_channels[accepted_channel] = {'owner': accepted_owner, 'members': accepted_members}
                         # Optional: Update known_peers from member list here if needed

                    # --- Handle Join Error/Rejection (I requested to join) ---
                    elif msg_type == 'join_error':
                        failed_channel = message.get('channel_name', 'N/A')
                        reason = message.get('reason', 'Unknown')
                        print(f"\r{' ' * 60}\r[P2P RECV] Join Failed for '{failed_channel}': {reason}\n{prompt}", end="", flush=True)

                    # --- Handle other P2P message types (Placeholder) ---
                    # elif msg_type == 'channel_message': ...
                    # elif msg_type == 'member_update': ...

                    else:
                        # Default: Display other JSON messages
                        print(f"\r{' ' * 60}\r[P2P JSON from {actual_peer_name}]: Type: {msg_type}\n{prompt}", end="", flush=True)

                except json.JSONDecodeError:
                    # Handle non-JSON messages (print as raw)
                    raw_msg = message_json
                    print(f"\r{' ' * 60}\r[P2P RAW from {actual_peer_name}]: {raw_msg}\n{prompt}", end="", flush=True)
                except Exception as e:
                    print(f"\n[P2P ERROR] Processing message from {actual_peer_name}: {e}\nMsg: {message_json}")

            if not running: break # Check flag after processing buffer

    except ConnectionResetError: print(f"\n[P2P <<] Peer {actual_peer_name} connection reset.")
    except (socket.error, OSError) as e:
         if running: print(f"\n[P2P ERROR] Socket error with {actual_peer_name}: {e}")
    except UnicodeDecodeError: print(f"\n[P2P ERROR] Non-UTF8 stream from {actual_peer_name}.")
    except Exception as e: print(f"\n[P2P ERROR] Unexpected in handle_peer_connection: {e}"); import traceback; traceback.print_exc()
    finally:
        # print(f"[P2P CLOSE] Connection with {actual_peer_name} closed.")
        peer_socket.close()

def listen_for_peers(host, port):
    """Starts P2P listener thread."""
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

# --- Registry Listener ---
def listen_to_registry(registry_socket):
    """Listens for updates from the registry *after* successful registration."""
    global known_peers, running, MY_INFO, my_channels, available_channels_info # Added available_channels_info
    buffer = ""
    prompt = f"{MY_NAME}> "
    while running:
        try:
            data = registry_socket.recv(BUFFER_SIZE)
            if not data:
                if running: print("\n[CONNECTION LOST] Registry server closed the connection.")
                running = False; break
            buffer += data.decode('utf-8')

            while '\n' in buffer:
                message_json, buffer = buffer.split('\n', 1)
                if not message_json.strip(): continue

                try:
                    message = json.loads(message_json)
                    msg_type = message.get('type')

                    # --- Peer List/Join/Left (Keep as before) ---
                    if msg_type == 'peer_list':
                        print("\n[REGISTRY] Received peer list update.")
                        server_peers = message.get('peers', {})
                        with peer_list_lock: 
                            known_peers = {pid: info for pid, info in server_peers.items() 
                                         if info.get('name') != MY_NAME and not info.get('is_invisible', False)}
                    elif msg_type == 'peer_joined':
                         peer_id = message.get('id'); peer_info = message.get('peer_info'); peer_name = peer_info.get('name', 'Unk') if peer_info else 'Unk'
                         if peer_id and peer_info and peer_name != MY_NAME and not peer_info.get('is_invisible', False):
                             with peer_list_lock: known_peers[peer_id] = peer_info
                             print(f"\r{' ' * 60}\r[REGISTRY] Peer joined: {peer_name}\n{prompt}", end="", flush=True)
                    elif msg_type == 'peer_left':
                        peer_id = message.get('id')
                        if peer_id:
                             removed_name = "Unk"
                             with peer_list_lock:
                                 if peer_id in known_peers: removed_name = known_peers.pop(peer_id).get('name', 'Unk')
                             if removed_name != "Unk": print(f"\r{' ' * 60}\r[REGISTRY] Peer left: {removed_name}\n{prompt}", end="", flush=True)

                    # --- Channel Message Handling (Keep as before) ---
                    elif msg_type == 'channel_created':
                         c_name = message.get('channel_name'); c_owner = message.get('owner_name')
                         print(f"\r{' ' * 60}\r[REGISTRY] Channel '{c_name}' creation confirmed.\n{prompt}", end="", flush=True)
                         if c_owner == MY_NAME:
                             with my_channels_lock:
                                 if c_name not in my_channels:
                                     if not MY_INFO: update_my_info(MY_NAME, MY_P2P_PORT) # Ensure MY_INFO exists
                                     my_channels[c_name] = {'owner': MY_NAME, 'members': {MY_NAME: MY_INFO.copy()}}
                                     if not MY_INFO: print(f"[WARN] MY_INFO empty for channel {c_name}")
                    elif msg_type == 'channel_error':
                        error_msg = message.get('message', 'Unk'); c_name = message.get('channel_name', 'N/A')
                        print(f"\r{' ' * 60}\r[REGISTRY ERROR] Channel '{c_name}': {error_msg}\n{prompt}", end="", flush=True)
                    elif msg_type == 'new_channel_available':
                        c_name = message.get('channel_name'); c_owner = message.get('owner_name')
                        c_ip = message.get('owner_ip'); c_port = message.get('owner_p2p_port')
                        print(f"\r{' ' * 60}\r[REGISTRY] New channel available: '{c_name}' (Owner: {c_owner})\n{prompt}", end="", flush=True)
                        # Update the global available channels list
                        if c_name and c_owner and c_ip and c_port:
                             with available_channels_info_lock:
                                 available_channels_info[c_name] = {'owner_name': c_owner, 'owner_ip': c_ip, 'owner_p2p_port': c_port}

                    elif msg_type == 'channel_list':
                        print(f"\r{' ' * 60}\r[REGISTRY] Received list of all channels:")
                        channels_data = message.get('channels', {})
                        # Clear previous list and update with new data
                        with available_channels_info_lock:
                            available_channels_info.clear()
                            if channels_data:
                                for name, info in sorted(channels_data.items()):
                                    owner = info.get('owner_name','N/A')
                                    contact = f"{info.get('owner_ip','N/A')}:{info.get('owner_p2p_port','N/A')}"
                                    print(f"  - {name} (Owner: {owner} @ {contact})")
                                    # Store owner details for joining
                                    available_channels_info[name] = {
                                        'owner_name': owner,
                                        'owner_ip': info.get('owner_ip'),
                                        'owner_p2p_port': info.get('owner_p2p_port')
                                    }
                            else:
                                print("  (No channels currently registered)")
                        print(f"{prompt}", end="", flush=True)

                    elif msg_type == 'error':
                        error_msg = message.get('message', 'Unknown server error')
                        print(f"\r{' ' * 60}\r[SERVER ERROR] {error_msg}\n{prompt}", end="", flush=True)

                    elif msg_type == 'server_shutdown':
                        print(f"\r{' ' * 60}\r[REGISTRY] {message.get('message', 'Server Shutting Down')}\n{prompt}", end="", flush=True)
                        running = False # Trigger client shutdown

                    else: print(f"\r{' ' * 60}\r[REGISTRY UNHANDLED] Type: {msg_type}\n{prompt}", end="", flush=True)

                except json.JSONDecodeError: print(f"\r{' ' * 60}\r[REGISTRY ERROR] Invalid JSON\n{prompt}", end="", flush=True)
                except socket.error as e:
                    if running: print(f"\n[ERROR] Socket error in listener: {e}"); running = False; break
                except Exception as e: print(f"\n[ERROR] Processing registry msg: {e}\nMessage: {message_json}")

        except (ConnectionResetError, ConnectionAbortedError):
            if running: print("\n[CONNECTION LOST] Registry closed."); running = False
        except OSError as e:
            if running: print(f"\n[SOCKET ERROR] Registry listener: {e}"); running = False
        except Exception as e:
            if running: print(f"\n[ERROR] Receiving from registry: {e}"); running = False

    print("[THREAD] Registry listener finished.")
    running = False # Ensure main loop stops

# --- P2P Message Sending (Keep as before) ---
def send_p2p_message(target_name, message):
    """Sends a message directly to a peer's P2P listener."""
    target_info = get_peer_info_by_name(target_name)
    if not target_info: print(f"[P2P ERROR] Peer '{target_name}' not found."); return False
    ip, port = target_info.get('ip'), target_info.get('p2p_port')
    if not ip or not port: print(f"[P2P ERROR] Peer '{target_name}' incomplete info."); return False

    # print(f"[P2P >>] Sending to {target_name} ({ip}:{port})...") # Optional Verbose
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
             sock.settimeout(5.0); sock.connect((ip, port))
             sock.sendall(message.encode('utf-8')); return True
    except socket.timeout: print(f"[P2P ERROR] Connection to {target_name} timed out.")
    except ConnectionRefusedError: print(f"[P2P ERROR] Connection to {target_name} refused.")
    except OSError as e: print(f"[P2P ERROR] Network error sending to {target_name}: {e}")
    except Exception as e: print(f"[P2P ERROR] Unexpected error sending to {target_name}: {e}")
    return False

# --- Channel Functions ---
def create_channel(registry_socket, channel_name, channel_type):
    """Sends create channel request to registry."""
    if not MY_NAME: print("[ERROR] Cannot create channel: Name not set."); return False
    if not MY_INFO: print("[ERROR] Cannot create channel: Own info not known."); return False
    try:
        msg = {'type': 'create_channel',
               'channel_name': channel_name,
               'channel_type': channel_type,
               'owner_name': MY_NAME,
               'owner_ip': MY_INFO.get('ip'),
               'owner_p2p_port': MY_INFO.get('p2p_port')}
        registry_socket.sendall((json.dumps(msg) + "\n").encode('utf-8'))
        print(f"[CMD] Request sent for channel '{channel_name}'.")
        return True
    except Exception as e: print(f"[ERROR] Sending create request: {e}"); return False

# --- MODIFIED Join Channel Function ---
# --- MODIFIED Join Channel Function ---
def join_channel(channel_name_to_join):
    """
    Checks local state, finds owner from available_channels_info,
    and sends P2P join request.
    """
    global available_channels_info, MY_NAME, MY_INFO, my_channels # Need access to local channels too

    if not channel_name_to_join:
        print("[JOIN ERROR] Channel name required.")
        return False

    # --- NEW: Check if already in the channel locally ---
    with my_channels_lock:
        if channel_name_to_join in my_channels:
            # Check if we are owner or member
            role = "owner" if my_channels[channel_name_to_join].get('owner') == MY_NAME else "member"
            print(f"[JOIN INFO] You are already a {role} of channel '{channel_name_to_join}'.")
            return True # Indicate no further action needed for joining
    # --- END NEW Check ---

    # 1. Find owner details from the globally stored list
    owner_info = None
    with available_channels_info_lock:
        channel_data = available_channels_info.get(channel_name_to_join)
        if channel_data:
            owner_info = channel_data
        else:
            print(f"[JOIN ERROR] Channel '{channel_name_to_join}' not found in available list.")
            print("[INFO] Use /list_channels to refresh.")
            return False

    owner_name = owner_info.get('owner_name')
    # Owner IP/Port needed by send_p2p_message's lookup, ensure they exist
    owner_ip = owner_info.get('owner_ip')
    owner_port = owner_info.get('owner_p2p_port')

    if not owner_name or not owner_ip or not owner_port:
        print(f"[JOIN ERROR] Incomplete owner information for channel '{channel_name_to_join}'. Cannot join.")
        return False

    # Check if trying to join own channel (should have been caught by local check above, but double-check)
    if owner_name == MY_NAME:
        print(f"[JOIN INFO] You are the owner of '{channel_name_to_join}'.")
        # Could potentially sync local state if needed here
        return True

    # 2. Ensure own info is available
    if not MY_INFO or not MY_NAME:
        print("[JOIN ERROR] Cannot join: Your own info is missing.")
        return False

    # 3. Construct the P2P message for the owner
    join_req_msg = {
        'type': 'request_join_channel',
        'channel_name': channel_name_to_join,
        'requester_name': MY_NAME,
        'requester_ip': MY_INFO.get('ip'),
        'requester_p2p_port': MY_INFO.get('p2p_port')
    }
    join_req_json = json.dumps(join_req_msg) + "\n"

    # 4. Send P2P message directly to the owner's name
    print(f"[JOIN] Sending join request for '{channel_name_to_join}' to owner '{owner_name}'...")
    # send_p2p_message will look up owner_name. Ensure owner_info is in known_peers or channel members if needed.
    # If owner might not be in known_peers yet, we could try sending directly to IP/Port from available_channels_info,
    # but using the name lookup is generally preferred if the peer is likely known.
    success = send_p2p_message(owner_name, join_req_json)

    if success: print(f"[JOIN] Request sent. Waiting for acceptance from {owner_name}.")
    else: print(f"[JOIN ERROR] Failed to send request to owner {owner_name}.")
    return success

# --- Initial Login/Guest Function (Keep as before) ---
def login_or_guest(host, port):
    """Handles initial login/guest choice and registry connection/ack."""
    global MY_NAME, MY_ID, is_guest, MY_INFO, running
    registry_socket = None
    while running:
        try:
            print("\n--- Welcome ---\n  /guest <name>\n  /login <name> <id>\n  (Ctrl+C to exit)")
            cmd = input("> ").strip()
            if not cmd: continue
            cmd_lower = cmd.lower()
            login_data = None

            if cmd_lower.startswith('/guest '):
                parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
                if name and 0 < len(name) <= 50 and ' ' not in name:
                    MY_NAME = name; MY_ID = None; is_guest = True
                    update_my_info(MY_NAME, MY_P2P_PORT) # Need info before sending
                    login_data = {'type': 'guest_login','name': MY_NAME,'ip': MY_INFO.get('ip'),'p2p_port': MY_P2P_PORT}
                else: print("[ERROR] Invalid guest name.")
            elif cmd_lower.startswith('/login '):
                parts = cmd.split(' ', 2)
                name = parts[1].strip() if len(parts)>1 else None
                id_str = parts[2].strip() if len(parts)>2 else None
                if name and 0 < len(name) <= 50 and ' ' not in name and id_str and id_str.isdigit():
                     MY_NAME = name; MY_ID = id_str; is_guest = False
                     update_my_info(MY_NAME, MY_P2P_PORT) # Need info before sending
                     login_data = {'type': 'user_login','name': MY_NAME,'id': MY_ID,'ip': MY_INFO.get('ip'),'p2p_port': MY_P2P_PORT}
                else: print("[ERROR] Invalid login format or details.")
            else: print("[ERROR] Unknown command.")

            if login_data:
                try:
                    print(f"[CONNECTING] to {host}:{port}...")
                    registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM); registry_socket.settimeout(10.0); registry_socket.connect((host, port))
                    print("[CONNECTED] Sending identification..."); 
                    login_data['is_invisible'] = is_invisible  # Add invisible status to login data
                    registry_socket.sendall((json.dumps(login_data) + "\n").encode('utf-8'))
                    print("[WAITING] for acknowledgement..."); buffer = ""; ack_received = False; success = False
                    # Increase ack timeout slightly?
                    registry_socket.settimeout(20.0)
                    while not ack_received:
                        try:
                            data = registry_socket.recv(BUFFER_SIZE);
                            if not data: print("[ERROR] Connection closed by server."); ack_received=True; success=False; break
                            buffer += data.decode('utf-8')
                            if '\n' in buffer:
                                resp_json, buffer = buffer.split('\n', 1); response = json.loads(resp_json)
                                r_type = response.get('type')
                                if r_type == 'login_ack' or r_type == 'guest_ack':
                                    print(f"[SUCCESS] {response.get('message')}"); server_ip = response.get('client_ip')
                                    if server_ip: update_my_info(MY_NAME, MY_P2P_PORT, ip=server_ip) # Update with server IP
                                    ack_received=True; success=True; registry_socket.settimeout(None); return registry_socket, True
                                elif r_type == 'error':
                                    print(f"[FAILED] {response.get('message')}"); ack_received=True; success=False; break # Break inner loop only
                                else: print(f"[WARNING] Unexpected ack response: {r_type}")
                        except socket.timeout: print("[ERROR] Timeout waiting for ack."); ack_received=True; success=False; break
                        except (socket.error, json.JSONDecodeError, UnicodeDecodeError) as e: print(f"[ERROR] Ack error: {e}"); ack_received=True; success=False; break
                    if not success: # If inner loop broke without success
                        if registry_socket: registry_socket.close(); registry_socket = None
                        print("[INFO] Please try again."); continue # Go back to command prompt
                except socket.timeout: print(f"[ERROR] Connection to {host}:{port} timed out."); time.sleep(1); continue
                except socket.error as e: print(f"[ERROR] Connect error: {e}"); time.sleep(1); continue
                except Exception as e: print(f"[CRITICAL] Login error: {e}"); return None, False # Exit client

        except (EOFError, KeyboardInterrupt): print("\n[EXITING]"); running = False; break # Break outer loop

    if registry_socket: registry_socket.close()
    return None, False # Return failure if loop broken by Ctrl+C


# --- Main Client Function (Keep general structure) ---
def start_p2p_client():
    global running, MY_NAME, MY_INFO # other globals set in login_or_guest

    print(f"[INFO] Starting P2P listener on {LISTEN_HOST}:{MY_P2P_PORT}...")
    p2p_listener_thread = threading.Thread(target=listen_for_peers, args=(LISTEN_HOST, MY_P2P_PORT), daemon=True)
    p2p_listener_thread.start()
    time.sleep(0.3)
    if not running: print("[CRITICAL] P2P listener failed. Exiting."); return

    registry_socket, login_success = login_or_guest(REGISTRY_HOST, REGISTRY_PORT)
    if not login_success:
        print("[EXITING] Login/Guest registration failed."); running = False; time.sleep(0.5); return

    # --- Logged In ---
    print("-" * 30 + f"\n--- Welcome, {'Guest ' if is_guest else ''}{MY_NAME} " + (f"(ID: {MY_ID})" if not is_guest else "") + " ---")
    print(f"Your Info: {MY_INFO}")
    print("--- Commands: /list /myinfo /msg /create /join /list_channels /my_channels /members /quit /invisible /offline /online ---")
    print("-" * 30)

    registry_listener_thread = threading.Thread(target=listen_to_registry, args=(registry_socket,), daemon=True)
    registry_listener_thread.start()

    # --- Main Command Loop ---
    while running:
        try:
             prompt = f"{MY_NAME}> "; cmd = input(prompt).strip()
        except (EOFError, KeyboardInterrupt): cmd = "/quit"
        if not running: break
        if not cmd: continue
        cmd_lower = cmd.lower()
        
        # Add global declarations at the beginning of the loop
        global is_invisible, is_offline

        if cmd_lower == '/quit': running = False; break
        elif cmd_lower == '/invisible':
            is_invisible = not is_invisible
            status = "invisible" if is_invisible else "visible"
            print(f"[STATUS] You are now {status}")
            # Send update to registry
            try:
                update_msg = {'type': 'update_visibility', 'is_invisible': is_invisible}
                registry_socket.sendall((json.dumps(update_msg) + "\n").encode('utf-8'))
            except Exception as e:
                print(f"[ERROR] Failed to update visibility status: {e}")
                is_invisible = not is_invisible  # Revert the change if update failed
        elif cmd_lower == '/offline':
            if not is_offline:
                is_offline = True
                print("[STATUS] You are now offline. You cannot use other features until you go online.")
                try:
                    update_msg = {'type': 'update_offline_status', 'is_offline': True}
                    registry_socket.sendall((json.dumps(update_msg) + "\n").encode('utf-8'))
                except Exception as e:
                    print(f"[ERROR] Failed to update offline status: {e}")
                    is_offline = False
        elif cmd_lower == '/online':
            if is_offline:
                is_offline = False
                print("[STATUS] You are now online. You can use all features again.")
                try:
                    update_msg = {'type': 'update_offline_status', 'is_offline': False}
                    registry_socket.sendall((json.dumps(update_msg) + "\n").encode('utf-8'))
                except Exception as e:
                    print(f"[ERROR] Failed to update offline status: {e}")
                    is_offline = True
        elif is_offline:
            print("[ERROR] You are offline. Use '/online' to use other features.")
            continue
        elif cmd_lower == '/list': # List known logged-in peers
            print("[KNOWN PEERS (Logged-in)]:")
            with peer_list_lock: peers = sorted(known_peers.values(), key=lambda x: x.get('name', 'z'))
            if peers: [print(f"  - {p.get('name','?')} ({p.get('ip','?')}:{p.get('p2p_port','?')})") for p in peers]
            else: print("  (No other peers known)")
        elif cmd_lower == '/myinfo': print(f"[YOUR INFO]: {MY_INFO} | Status: {'Guest' if is_guest else f'User (ID: {MY_ID})'}")
        elif cmd_lower.startswith('/msg '):
             parts = cmd.split(' ', 2);
             if len(parts)==3:
                 if parts[1] == MY_NAME: print("[CMD ERROR] Cannot /msg yourself.")
                 else: threading.Thread(target=send_p2p_message, args=(parts[1], parts[2]), daemon=True).start() # Send in thread
             else: print("[CMD ERROR] Usage: /msg <name> <message>")

        elif cmd_lower.startswith('/create '):
            parts = cmd.split(' ', 2); 
            if len(parts) == 3:
                name = parts[1].strip()
                channel_type = parts[2].strip()
                if name and channel_type: 
                    create_channel(registry_socket, name, channel_type)
                else: print("[CMD ERROR] Usage: /create <channel_name> <channel_type>")
            else:
                None

        elif cmd_lower.startswith('/join '): # Use the join function
            parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
            if name: join_channel(name) # Calls the updated join_channel
            else: print("[CMD ERROR] Usage: /join <channel_name>")
        elif cmd_lower == '/list_channels': # Request list from registry
             print("[CMD] Requesting channel list...");
             try: registry_socket.sendall((json.dumps({'type': 'request_channel_list'}) + "\n").encode('utf-8'))
             except Exception as e: print(f"[ERROR] Sending request: {e}")
        elif cmd_lower == '/my_channels': # List channels this client is in (local)
             print("[YOUR CHANNELS]:");
             with my_channels_lock: channels = sorted(my_channels.items())
             if channels:
                 for name, data in channels:
                     role = "Owner" if data.get('owner') == MY_NAME else "Member"; count = len(data.get('members', {})) if role=="Owner" else "?"; owner=data.get('owner','?')
                     print(f"  - {name} ({role}, Owner: {owner}, {count} members)")
             else: print("  (Not in any channels)")
        elif cmd_lower.startswith('/members '): # List members if owner (local)
             parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
             if name:
                 with my_channels_lock: channel_data = my_channels.get(name)
                 if channel_data:
                     if channel_data.get('owner') == MY_NAME:
                         print(f"[MEMBERS of '{name}']: (Owner)"); members = sorted(channel_data.get('members', {}).keys())
                         if members: [print(f"  - {m} {'(You)' if m == MY_NAME else ''}") for m in members]
                         else: print("  (List empty?)")
                     else: print(f"[INFO] Not owner of '{name}'. Owner ({channel_data.get('owner','?')}) manages list.")
                 else: print(f"[CMD ERROR] Not part of channel '{name}'.")
             else: print("[CMD ERROR] Usage: /members <channel_name>")
        else: print("[CMD ERROR] Unknown command.")

    # --- Cleanup ---
    if running: print("\n[EXITING] Shutting down...")
    running = False
    print("[CLOSING] Registry connection...")
    try:
        if registry_socket: registry_socket.shutdown(socket.SHUT_RDWR); registry_socket.close()
    except Exception: pass
    print("[INFO] Waiting for threads..."); time.sleep(0.5)
    print("[CLOSED] Client shut down.")

# --- Entry Point ---
if __name__ == "__main__":
    start_p2p_client()