# --- p2p_client.py ---
import socket
import threading
import sys
import time
import json
import random
import ipaddress # Keep if needed for IP validation elsewhere, otherwise optional
import datetime
from datetime import timezone # Explicitly import timezone

# --- Configuration ---
REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 9999
LISTEN_HOST = '0.0.0.0' # Listen on all available interfaces
MY_P2P_PORT = random.randint(10000, 60000) # Random port for P2P listening
BUFFER_SIZE = 4096

# --- Shared State ---
peer_list_lock = threading.Lock()
known_peers = {} # Stores peer info from registry {peer_id: {'name':.., 'ip':.., 'p2p_port':..}}

my_channels_lock = threading.Lock()
# Channels client owns or is in: { channel_name: {'owner': name, 'members': {name: info}} }
my_channels = {}

available_channels_info_lock = threading.Lock()
# Info about channels available on the registry: { channel_name: {'owner_name':.., 'owner_ip':.., 'owner_p2p_port':..} }
available_channels_info = {}

channel_logs_lock = threading.Lock()
# Local message logs for channels owned by this client: { channel_name: [{'timestamp':.., 'sender':.., 'content':..}] }
channel_message_logs = {}

# --- Application Control ---
running = True # Global flag for main loop and threads
is_offline = False # Tracks network connection state

# --- Identity and Info ---
MY_NAME = None
MY_ID = None # For registered users
is_guest = None
MY_INFO = {} # Own IP, P2P port, Name

# --- Stored Info for Reconnect ---
initial_login_info = {} # Stores {'type': 'guest'/'user', 'name': ..., 'id': ...}

# --- Network & Thread Management ---
registry_socket = None # Holds the connection to the registry server
p2p_listener_thread = None
registry_listener_thread = None
stop_p2p_listener_event = threading.Event() # Event to signal P2P listener stop
stop_registry_listener_event = threading.Event() # Event to signal Registry listener stop

# --- Visibility ---
is_invisible = False # Tracks user's visibility choice

# --- Helper Functions ---
def get_local_ip():
    """Try to get a non-loopback local IP address."""
    try:
        # Connect to an external host (doesn't send data) to find preferred outbound IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.1)
        s.connect(('8.8.8.8', 80)) # Google DNS as target
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            # Fallback: Get hostname and resolve it
            return socket.gethostbyname(socket.gethostname())
        except socket.gaierror:
            # Ultimate fallback: Loopback address
            return '127.0.0.1'

def update_my_info(name, p2p_port, ip=None):
    """Updates MY_INFO with determined or server-provided IP."""
    global MY_INFO
    current_ip = ip if ip else get_local_ip()
    new_info = {'ip': current_ip, 'p2p_port': p2p_port, 'name': name}
    if MY_INFO != new_info: # Only update if changed
        MY_INFO = new_info
        source = "(from server)" if ip else "(determined locally)"
        print(f"[INFO] Updated own info {source}: {MY_INFO}")

def get_peer_name_by_address(ip, port):
    """Looks up peer name from known_peers or returns IP:Port."""
    # Check self first
    if MY_INFO.get('ip') == ip and MY_INFO.get('p2p_port') == port:
        return f"{MY_NAME} (Self)"

    # Check known peers from registry
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('ip') == ip and info.get('p2p_port') == port:
                return info.get('name', f"{ip}:{port}") # Return name or fallback

    # Fallback: Check local channel members (less reliable for general lookup)
    with my_channels_lock:
        for channel in my_channels.values():
            for member_name, member_info in channel.get('members', {}).items():
                if member_info.get('ip') == ip and member_info.get('p2p_port') == port:
                    return member_name # Found in a channel

    return f"{ip}:{port}" # Ultimate fallback: IP:Port

def get_peer_info_by_name(name):
    """Looks up peer IP/Port from known_peers or local channels."""
    if name == MY_NAME:
        return MY_INFO

    # Prioritize known_peers list from registry
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('name') == name:
                return info # Return full info dict

    # Fallback: Check local channel member lists
    with my_channels_lock:
        for channel in my_channels.values():
            member_info = channel.get('members', {}).get(name)
            if member_info:
                return member_info # Return info stored in channel member list

    return None # Not found

# --- P2P Listener ---
def listen_for_peers(host, port, stop_event):
    """Starts P2P listener thread and stops when event is set."""
    p2p_server_socket = None # Initialize
    try:
        p2p_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        p2p_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        p2p_server_socket.bind((host, port))
        p2p_server_socket.listen(5)
        print(f"[P2P LISTENING] Ready for P2P connections on {host}:{port}")

        p2p_server_socket.settimeout(1.0) # Use timeout for periodic checks

        # Loop while running flag is true AND stop event is NOT set
        while running and not stop_event.is_set():
            try:
                peer_socket, peer_address = p2p_server_socket.accept()
                # Start handler thread (daemon=True means it won't block exit)
                peer_thread = threading.Thread(target=handle_peer_connection, args=(peer_socket, peer_address), daemon=True)
                peer_thread.start()
            except socket.timeout:
                continue # Timeout is expected, just loop again to check flags
            except OSError as e: # Handle socket closed during accept
                 if not stop_event.is_set() and running: # Only log if not intentional stop
                     print(f"[P2P LISTENER ERROR] Accept error: {e}")
                 break # Exit loop if socket is closed unexpectedly
            except Exception as e:
                if running and not stop_event.is_set():
                    print(f"[P2P LISTENER CRITICAL ERROR] Unexpected error: {e}")
                break

    except OSError as e:
        # Only report bind error if not shutting down
        if running and not stop_event.is_set():
            print(f"[P2P LISTENER BIND ERROR] Could not bind P2P listener to {host}:{port} - {e}.")
        # Consider setting running = False here if bind fails critically?
    except Exception as e:
        if running and not stop_event.is_set():
             print(f"[P2P LISTENER CRITICAL] Other error: {e}")
    finally:
        print("[P2P LISTENING] Shutting down P2P listener.")
        # Close the listening socket when the thread exits
        if p2p_server_socket:
            try:
                p2p_server_socket.close()
            except Exception as e:
                print(f"[P2P LISTENER WARN] Error closing listener socket: {e}")

# --- P2P Connection Handler ---
def handle_peer_connection(peer_socket, peer_address):
    """Handles incoming P2P connection and processes messages."""
    actual_peer_name = get_peer_name_by_address(peer_address[0], peer_address[1])
    prompt = f"({ 'OFFLINE' if is_offline else MY_NAME })> " # Get current prompt style
    buffer = ""

    try:
        while running: # Check main running flag
            try:
                data = peer_socket.recv(BUFFER_SIZE)
                if not data:
                    # print(f"\n[P2P <<] Peer {actual_peer_name} disconnected.")
                    break # Peer closed connection
            except ConnectionResetError:
                # print(f"\n[P2P <<] Peer {actual_peer_name} connection reset.")
                break
            except socket.error as e:
                # print(f"\n[P2P ERROR] Socket error with {actual_peer_name}: {e}")
                break # Assume connection is broken

            try:
                buffer += data.decode('utf-8')
            except UnicodeDecodeError:
                print(f"\n[P2P ERROR] Non-UTF8 stream from {actual_peer_name}.")
                buffer = "" # Clear invalid buffer
                continue

            # Process complete messages (newline delimited)
            while '\n' in buffer and running:
                message_json, buffer = buffer.split('\n', 1)
                if not message_json.strip(): continue

                try:
                    message = json.loads(message_json)
                    msg_type = message.get('type')
                    current_prompt = f"({ 'OFFLINE' if is_offline else MY_NAME })> " # Refresh prompt

                    # --- Handle Channel Join Request (Owner receives) ---
                    if msg_type == 'request_join_channel':
                        # ... (Keep existing logic from previous version) ...
                        req_channel = message.get('channel_name')
                        req_name = message.get('requester_name')
                        req_ip = message.get('requester_ip')
                        req_port = message.get('requester_p2p_port')

                        if req_name: actual_peer_name = req_name # Update name
                        print(f"\r{' ' * 60}\r[P2P RECV] Join request for '{req_channel}' from '{req_name}'\n{current_prompt}", end="", flush=True)

                        if not (req_channel and req_name and req_ip and req_port):
                             print(f"[P2P WARNING] Invalid join request from {actual_peer_name}: missing fields.")
                             continue

                        with my_channels_lock:
                            if req_channel in my_channels and my_channels[req_channel].get('owner') == MY_NAME:
                                channel_data = my_channels[req_channel]
                                members_dict = channel_data.setdefault('members', {})
                                requester_info = {'name': req_name, 'ip': req_ip, 'p2p_port': req_port}

                                if req_name not in members_dict:
                                     print(f"[CHANNEL] Adding '{req_name}' to your channel '{req_channel}'.")
                                else: # Already member, just update info maybe
                                     print(f"[CHANNEL INFO] '{req_name}' (already member) requested join. Updating info.")
                                members_dict[req_name] = requester_info # Add or update

                                # Send acceptance
                                accept_response = {'type': 'join_channel_accepted','channel_name': req_channel,'owner_name': MY_NAME,'members': members_dict}
                                accept_thread = threading.Thread(target=send_p2p_message, args=(req_name, json.dumps(accept_response) + '\n'), daemon=True)
                                accept_thread.start()
                                print(f"[P2P >>] Sent join acceptance for '{req_channel}' to '{req_name}'.")
                            # ... (handle other cases: not owner, unknown channel)

                    # --- Handle Join Acceptance (Requester receives) ---
                    elif msg_type == 'join_channel_accepted':
                        # ... (Keep existing logic from previous version) ...
                        accepted_channel = message.get('channel_name')
                        accepted_owner = message.get('owner_name')
                        accepted_members = message.get('members', {})
                        print(f"\r{' ' * 60}\r[P2P RECV] Joined channel '{accepted_channel}' (Owner: {accepted_owner})!")
                        print(f"[CHANNEL] Members: {list(accepted_members.keys())}\n{current_prompt}", end="", flush=True)
                        with my_channels_lock:
                             my_channels[accepted_channel] = {'owner': accepted_owner, 'members': accepted_members}

                    # --- Handle Join Error/Rejection (Requester receives) ---
                    elif msg_type == 'join_error':
                        # ... (Keep existing logic from previous version) ...
                        failed_channel = message.get('channel_name', 'N/A')
                        reason = message.get('reason', 'Unknown')
                        print(f"\r{' ' * 60}\r[P2P RECV] Join Failed for '{failed_channel}': {reason}\n{current_prompt}", end="", flush=True)

                    # --- Handle Direct Channel Message (Member/Owner receives from Owner) ---
                    elif msg_type == 'channel_message':
                        # ... (Keep existing logic from previous version) ...
                        channel = message.get('channel_name')
                        sender = message.get('sender')
                        content = message.get('content')
                        timestamp = message.get('timestamp', 'N/A')

                        if channel and sender and content:
                            with my_channels_lock: is_member = channel in my_channels
                            if is_member:
                                try:
                                    ts_obj = datetime.datetime.fromisoformat(timestamp) # Use standard parsing
                                    time_str = ts_obj.strftime('%H:%M:%S')
                                except (ValueError, TypeError): time_str = "time?"
                                display_sender = f"{sender} (You)" if sender == MY_NAME else sender
                                print(f"\r{' ' * 60}\r[{channel} @ {time_str}] {display_sender}: {content}\n{current_prompt}", end="", flush=True)
                            # else: Ignore message for channel not joined
                        # else: Invalid format

                    # --- Handle Forward Request (Owner receives from Member) ---
                    elif msg_type == 'forward_to_owner':
                        # ... (Keep existing logic from previous version) ...
                        channel = message.get('channel_name')
                        original_sender = message.get('original_sender')
                        msg_content = message.get('content')
                        timestamp = message.get('timestamp', datetime.datetime.now(timezone.utc).isoformat())

                        if not (channel and original_sender and msg_content is not None):
                            print(f"\n[P2P WARNING] Invalid 'forward_to_owner' message: missing required fields.")
                            continue

                        is_owner = False
                        members_to_forward = {}
                        with my_channels_lock:
                            if channel in my_channels and my_channels[channel].get('owner') == MY_NAME:
                                is_owner = True
                                members_to_forward = my_channels[channel].get('members', {}).copy()

                        if is_owner:
                            # Log locally
                            with channel_logs_lock:
                                log_entry = {'timestamp': timestamp, 'sender': original_sender, 'content': msg_content}
                                channel_message_logs.setdefault(channel, []).append(log_entry)
                            # Display locally for owner
                            try:
                                ts_obj = datetime.datetime.fromisoformat(timestamp)
                                time_str = ts_obj.strftime('%H:%M:%S')
                            except (ValueError, TypeError): time_str = "time?"
                            print(f"\r{' ' * 60}\r[{channel} @ {time_str}] {original_sender}: {msg_content}\n{current_prompt}", end="", flush=True) # Display owner sees

                            # Prepare forward payload
                            forward_payload = {
                                'type': 'channel_message', 'channel_name': channel,
                                'sender': original_sender, 'content': msg_content,
                                'timestamp': timestamp
                            }
                            forward_json = json.dumps(forward_payload) + "\n"
                            # Forward to others
                            forward_count = 0
                            for member_name in members_to_forward:
                                if member_name != MY_NAME and member_name != original_sender:
                                    fwd_thread = threading.Thread(target=send_p2p_message, args=(member_name, forward_json), daemon=True)
                                    fwd_thread.start()
                                    forward_count += 1
                            # print(f"[P2P FORWARD] Forwarded '{channel}' msg from '{original_sender}' to {forward_count} others.")
                        # else: Ignore if not owner

                    else:
                        # Handle other P2P messages if needed
                        print(f"\r{' ' * 60}\r[P2P UNHANDLED from {actual_peer_name}]: Type: {msg_type}\n{current_prompt}", end="", flush=True)

                except json.JSONDecodeError:
                    print(f"\n[P2P ERROR] Invalid JSON from {actual_peer_name}: {message_json}")
                except Exception as e:
                    print(f"\n[P2P ERROR] Processing message from {actual_peer_name}: {e}")
                    import traceback
                    traceback.print_exc() # More detailed error

    except Exception as e:
        # Catch unexpected errors in the main loop of the handler
        if running: # Avoid error message if shutting down
             print(f"\n[P2P HANDLER CRITICAL] Error with {actual_peer_name}: {e}")
             # traceback.print_exc() # Optional detailed traceback
    finally:
        # print(f"[P2P CLOSE] Connection handler finished for {actual_peer_name}.")
        try:
            peer_socket.shutdown(socket.SHUT_RDWR)
        except: pass # Ignore errors if already closed
        try:
            peer_socket.close()
        except: pass


# --- Registry Listener ---
def listen_to_registry(reg_socket, stop_event):
    """Listens for updates from the registry and stops when event is set."""
    global running, known_peers, MY_INFO, my_channels, available_channels_info # Keep globals needed

    if not reg_socket:
        print("[REGISTRY LISTENER] Error: No registry socket provided.")
        return

    buffer = ""
    try:
        reg_socket.settimeout(1.0) # Use timeout for periodic checks
        # Loop while running flag is true AND stop event is NOT set
        while running and not stop_event.is_set():
            try:
                data = reg_socket.recv(BUFFER_SIZE)
                if not data:
                    if running and not stop_event.is_set():
                        print("\n[CONNECTION LOST] Registry server closed the connection.")
                    break # Exit loop

                try: buffer += data.decode('utf-8')
                except UnicodeDecodeError: print("\n[REGISTRY ERROR] Invalid UTF-8 data."); buffer=""; continue

                while '\n' in buffer:
                    if not running or stop_event.is_set(): break # Check flags again
                    message_json, buffer = buffer.split('\n', 1)
                    if not message_json.strip(): continue

                    current_prompt = f"({ 'OFFLINE' if is_offline else MY_NAME })> " # Refresh prompt style
                    try:
                        message = json.loads(message_json)
                        msg_type = message.get('type')

                        # --- Peer List Handling ---
                        if msg_type == 'peer_list':
                           # ... (Keep existing logic) ...
                           server_peers = message.get('peers', {})
                           print(f"\r{' ' * 60}\r[CLIENT] Received updated peer list from registry.")
                           with peer_list_lock: known_peers = server_peers
                           print("[CLIENT] Currently known available peers:")
                           sorted_peer_list = sorted(known_peers.values(), key=lambda p: p.get('name', 'z').lower())
                           if sorted_peer_list:
                               for p_info in sorted_peer_list:
                                   name=p_info.get('name','?'); ip=p_info.get('ip','?'); port=p_info.get('p2p_port','?')
                                   status=p_info.get('login_status','?'); status_str=f"({status})"
                                   print(f"  - {name} {status_str} ({ip}:{port})")
                           else: print("  (Registry reported no other available peers)")
                           print(f"{current_prompt}", end="", flush=True)

                        # --- Peer Joined/Left ---
                        elif msg_type == 'peer_joined':
                            # ... (Keep existing logic) ...
                            peer_id=message.get('id'); info=message.get('peer_info'); name=info.get('name','Unk') if info else 'Unk'
                            if peer_id and info and name != MY_NAME and not info.get('is_invisible',False):
                                with peer_list_lock: known_peers[peer_id] = info
                                print(f"\r{' ' * 60}\r[REGISTRY] Peer joined: {name}\n{current_prompt}", end="", flush=True)
                        elif msg_type == 'peer_left':
                            # ... (Keep existing logic) ...
                            peer_id = message.get('id')
                            if peer_id:
                                removed_name = "Unk"
                                with peer_list_lock:
                                    if peer_id in known_peers: removed_name = known_peers.pop(peer_id).get('name', 'Unk')
                                if removed_name != "Unk":
                                    print(f"\r{' ' * 60}\r[REGISTRY] Peer left: {removed_name}\n{current_prompt}", end="", flush=True)

                        # --- Channel Messages ---
                        elif msg_type == 'channel_created':
                            # ... (Keep existing logic, including updating local my_channels if owner) ...
                            c_name=message.get('channel_name'); c_owner=message.get('owner_name')
                            print(f"\r{' ' * 60}\r[REGISTRY] Channel '{c_name}' creation confirmed.\n{current_prompt}", end="", flush=True)
                            if c_owner == MY_NAME:
                                with my_channels_lock:
                                    if c_name not in my_channels:
                                         if not MY_INFO: update_my_info(MY_NAME, MY_P2P_PORT) # Ensure info exists
                                         my_channels[c_name] = {'owner': MY_NAME, 'members': {MY_NAME: MY_INFO.copy()}}
                        elif msg_type == 'channel_error':
                             # ... (Keep existing logic) ...
                             error_msg=message.get('message','Unk'); c_name=message.get('channel_name','N/A')
                             print(f"\r{' ' * 60}\r[REGISTRY ERROR] Channel '{c_name}': {error_msg}\n{current_prompt}", end="", flush=True)
                        elif msg_type == 'new_channel_available':
                             # ... (Keep existing logic, updating available_channels_info) ...
                             c_name=message.get('channel_name'); c_owner=message.get('owner_name')
                             c_ip=message.get('owner_ip'); c_port=message.get('owner_p2p_port')
                             print(f"\r{' ' * 60}\r[REGISTRY] New channel available: '{c_name}' (Owner: {c_owner})\n{current_prompt}", end="", flush=True)
                             if c_name and c_owner and c_ip and c_port:
                                  with available_channels_info_lock:
                                       available_channels_info[c_name] = {'owner_name': c_owner, 'owner_ip': c_ip, 'owner_p2p_port': c_port}
                        elif msg_type == 'channel_list':
                             # ... (Keep existing logic, updating available_channels_info) ...
                            print(f"\r{' ' * 60}\r[REGISTRY] Received list of all channels:")
                            channels_data = message.get('channels', {})
                            with available_channels_info_lock:
                                available_channels_info.clear()
                                if channels_data:
                                    for name, info in sorted(channels_data.items()):
                                        owner=info.get('owner_name','N/A'); contact=f"{info.get('owner_ip','N/A')}:{info.get('owner_p2p_port','N/A')}"
                                        print(f"  - {name} (Owner: {owner} @ {contact})")
                                        available_channels_info[name] = info # Store full info
                                else: print("  (No channels currently registered)")
                            print(f"{current_prompt}", end="", flush=True)

                        # --- Other Server Messages ---
                        elif msg_type == 'error':
                            # ... (Keep existing logic) ...
                             error_msg = message.get('message', 'Unknown server error')
                             print(f"\r{' ' * 60}\r[SERVER ERROR] {error_msg}\n{current_prompt}", end="", flush=True)
                        elif msg_type == 'server_shutdown':
                            # ... (Keep existing logic) ...
                             print(f"\r{' ' * 60}\r[REGISTRY] {message.get('message', 'Server Shutting Down')}\n{current_prompt}", end="", flush=True)
                             running = False # Server shutdown means we stop too
                             # Signal offline state locally? Or just let it quit? Let quit for now.
                             break # Exit inner loop

                        else:
                            print(f"\r{' ' * 60}\r[REGISTRY UNHANDLED] Type: {msg_type}\n{current_prompt}", end="", flush=True)

                    except json.JSONDecodeError:
                         print(f"\r{' ' * 60}\r[REGISTRY ERROR] Invalid JSON received\n{current_prompt}", end="", flush=True)
                    except Exception as e:
                         print(f"\n[ERROR] Processing registry msg: {e}\nMessage: {message_json}\n{current_prompt}", end="", flush=True)

                if not running or stop_event.is_set(): break # Check after processing buffer

            except socket.timeout:
                continue # Expected, just loop again to check flags
            except (ConnectionResetError, ConnectionAbortedError, OSError) as e:
                if running and not stop_event.is_set():
                    print(f"\n[REGISTRY LISTENER ERROR] Connection error: {e}")
                break # Exit loop on connection error
            except Exception as e:
                if running and not stop_event.is_set():
                    print(f"\n[ERROR] Unexpected error receiving from registry: {e}")
                break # Exit loop on other errors

    finally:
        print("[THREAD] Registry listener finished.")
        # Socket closing is handled by the main thread when going offline/quitting

# --- P2P Message Sending ---
def send_p2p_message(target_name, message_json_with_newline):
    """Sends a pre-formatted JSON message string directly to a peer's P2P listener."""
    if is_offline: # Cannot send P2P while offline
        print(f"[P2P SEND ERROR] Cannot send to '{target_name}': Currently offline.")
        return False

    target_info = get_peer_info_by_name(target_name)
    if not target_info:
        print(f"[P2P SEND ERROR] Peer '{target_name}' not found.")
        return False

    ip, port = target_info.get('ip'), target_info.get('p2p_port')
    if not ip or not port:
        print(f"[P2P SEND ERROR] Peer '{target_name}' incomplete info (IP/Port missing).")
        return False

    # print(f"[P2P >>] Sending to {target_name} ({ip}:{port})...") # Optional Verbose
    p2p_send_socket = None
    try:
        p2p_send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        p2p_send_socket.settimeout(5.0) # Timeout for connection and send
        p2p_send_socket.connect((ip, port))
        p2p_send_socket.sendall(message_json_with_newline.encode('utf-8'))
        return True # Success
    except socket.timeout:
        print(f"[P2P SEND ERROR] Connection to {target_name} ({ip}:{port}) timed out.")
    except ConnectionRefusedError:
        print(f"[P2P SEND ERROR] Connection to {target_name} ({ip}:{port}) refused.")
    except OSError as e:
        print(f"[P2P SEND ERROR] Network error sending to {target_name}: {e}")
    except Exception as e:
        print(f"[P2P SEND ERROR] Unexpected error sending to {target_name}: {e}")
    finally:
        if p2p_send_socket:
            try: p2p_send_socket.close()
            except: pass # Ignore errors on close
    return False # Failure

# --- Channel Functions ---
def create_channel(reg_socket, channel_name, channel_type):
    """Sends create channel request to registry. Assumes already online."""
    if not MY_NAME or not MY_INFO:
        print("[ERROR] Cannot create channel: User/Own info missing."); return False
    if not reg_socket:
        print("[ERROR] Cannot create channel: Not connected to registry (Offline?)."); return False

    try:
        msg = {'type': 'create_channel', 'channel_name': channel_name,
               'channel_type': channel_type, 'owner_name': MY_NAME,
               'owner_ip': MY_INFO.get('ip'), 'owner_p2p_port': MY_INFO.get('p2p_port')}
        reg_socket.sendall((json.dumps(msg) + "\n").encode('utf-8'))
        print(f"[CMD] Channel creation request sent for '{channel_name}'.")
        return True
    except (socket.error, OSError) as e: print(f"[ERROR] Network error sending create request: {e}"); return False
    except Exception as e: print(f"[ERROR] Failed to send create request: {e}"); return False

def join_channel(channel_name_to_join):
    """Sends P2P join request to channel owner. Assumes already online."""
    global available_channels_info, MY_NAME, MY_INFO, my_channels

    if not channel_name_to_join: print("[JOIN ERROR] Channel name required."); return False

    # Check if already in channel locally
    with my_channels_lock:
        if channel_name_to_join in my_channels:
             role = "owner" if my_channels[channel_name_to_join].get('owner') == MY_NAME else "member"
             print(f"[JOIN INFO] You are already a {role} of channel '{channel_name_to_join}'.")
             return True

    # Find owner details from locally cached list
    owner_info = None
    with available_channels_info_lock:
        channel_data = available_channels_info.get(channel_name_to_join)
        if channel_data: owner_info = channel_data
        else: print(f"[JOIN ERROR] Channel '{channel_name_to_join}' not found in available list (/list_channels)."); return False

    owner_name = owner_info.get('owner_name')
    owner_ip = owner_info.get('owner_ip') # Needed for get_peer_info fallback
    owner_port = owner_info.get('owner_p2p_port') # Needed for get_peer_info fallback

    if not owner_name or not owner_ip or not owner_port: print(f"[JOIN ERROR] Incomplete owner info for '{channel_name_to_join}'."); return False
    if owner_name == MY_NAME: print(f"[JOIN INFO] You are the owner of '{channel_name_to_join}'."); return True
    if not MY_INFO or not MY_NAME: print("[JOIN ERROR] Cannot join: Your own info is missing."); return False

    # Construct P2P message
    join_req_msg = {'type': 'request_join_channel', 'channel_name': channel_name_to_join,
                    'requester_name': MY_NAME, 'requester_ip': MY_INFO.get('ip'),
                    'requester_p2p_port': MY_INFO.get('p2p_port')}
    join_req_json = json.dumps(join_req_msg) + "\n"

    # Send P2P message (will fail if offline via send_p2p_message check)
    print(f"[JOIN] Sending P2P join request for '{channel_name_to_join}' to owner '{owner_name}'...")
    success = send_p2p_message(owner_name, join_req_json)

    if success: print(f"[JOIN] Request sent. Waiting for acceptance from {owner_name}.")
    # else: Error already printed by send_p2p_message
    return success

# --- Send Channel Message (Handles Offline Owner) ---
def send_channel_msg(channel_name, msg_content, reg_socket):
    """Sends a message within a channel or handles local logging if offline owner."""
    global my_channels, MY_NAME, MY_INFO, channel_message_logs, is_offline

    # --- Check if currently offline ---
    if is_offline:
        with my_channels_lock:
            if channel_name not in my_channels: print(f"[MSG ERROR] Not in channel '{channel_name}'."); return False
            channel_data = my_channels[channel_name]
            is_owner = (channel_data.get('owner') == MY_NAME)

        if is_owner:
            # print(f"[MSG OFFLINE SEND] Logging message locally for channel '{channel_name}'.")
            timestamp_iso = datetime.datetime.now(timezone.utc).isoformat()
            # Log locally
            with channel_logs_lock:
                log_entry = {'timestamp': timestamp_iso, 'sender': MY_NAME, 'content': msg_content}
                channel_message_logs.setdefault(channel_name, []).append(log_entry)
            # Display locally
            try: ts_obj = datetime.datetime.fromisoformat(timestamp_iso); time_str = ts_obj.strftime('%H:%M:%S')
            except: time_str = "time?"
            print(f"[{channel_name} @ {time_str}] {MY_NAME} (Offline Log): {msg_content}")
            return True
        else: print("[MSG ERROR] Cannot send messages as a member while offline."); return False
    # --- End of Offline Handling ---

    # --- Proceed with ONLINE Sending Logic ---
    with my_channels_lock:
        if channel_name not in my_channels: print(f"[MSG ERROR] Not in channel '{channel_name}'."); return False
        channel_data = my_channels[channel_name]
        owner_name = channel_data.get('owner')
        members_dict = channel_data.get('members', {})
        is_owner = (owner_name == MY_NAME)

    if not MY_NAME or not MY_INFO: print("[MSG ERROR] Cannot send: User info missing."); return False

    timestamp_iso = datetime.datetime.now(timezone.utc).isoformat()

    if is_owner:
        # --- Owner Sending (Online) ---
        message_payload = {'type': 'channel_message', 'channel_name': channel_name,
                           'sender': MY_NAME, 'content': msg_content, 'timestamp': timestamp_iso }
        message_json = json.dumps(message_payload) + "\n"
        # Log locally
        with channel_logs_lock:
            log_entry = {'timestamp': timestamp_iso, 'sender': MY_NAME, 'content': msg_content}
            channel_message_logs.setdefault(channel_name, []).append(log_entry)
        # Send P2P to all members
        for member_name in members_dict:
             # Send in thread
             send_thread = threading.Thread(target=send_p2p_message, args=(member_name, message_json), daemon=True)
             send_thread.start()
        # Owner sees own message via P2P handler loopback/display

    else:
        # --- Member Sending (Online) ---
        message_payload = {'type': 'forward_to_owner', 'channel_name': channel_name,
                           'original_sender': MY_NAME, 'content': msg_content, 'timestamp': timestamp_iso }
        message_json = json.dumps(message_payload) + "\n"
        # Try sending P2P to owner first
        success = send_p2p_message(owner_name, message_json)
        if success:
             # Display own message immediately after successful P2P send to owner
             try: ts_obj = datetime.datetime.fromisoformat(timestamp_iso); time_str = ts_obj.strftime('%H:%M:%S')
             except: time_str = "time?"
             print(f"[{channel_name} @ {time_str}] {MY_NAME}: {msg_content}")
        else:
            # P2P failed - Fallback: Send to server for logging
            print(f"[MSG WARNING] Failed P2P send to owner '{owner_name}'. Sending to server log...")
            if reg_socket:
                try:
                    reg_socket.sendall(message_json.encode('utf-8'))
                    print("[MSG INFO] Message forwarded to server log.")
                    # Display own message after successful server send
                    try: ts_obj = datetime.datetime.fromisoformat(timestamp_iso); time_str = ts_obj.strftime('%H:%M:%S')
                    except: time_str = "time?"
                    print(f"[{channel_name} @ {time_str}] {MY_NAME}: {msg_content}")
                except Exception as e: print(f"[MSG ERROR] Failed to send message to server log: {e}")
            else: print("[MSG ERROR] Failed P2P to owner and cannot reach server (offline?). Message not sent.")
    return True


# --- Initial Login/Guest Function ---
def login_or_guest(host, port):
    """Handles initial login/guest choice and registry connection/ack. Sets global socket."""
    global MY_NAME, MY_ID, is_guest, MY_INFO, running, initial_login_info, registry_socket

    temp_socket = None # Use temporary socket during login attempt
    while running:
        try:
            print("\n--- Welcome ---\n  /guest <name>\n  /login <name> <id>\n  (Ctrl+C to exit)")
            cmd = input("> ").strip()
            if not cmd: continue
            cmd_lower = cmd.lower()
            login_data = None
            potential_login_info = {}

            # --- Parse Command ---
            if cmd_lower.startswith('/guest '):
                parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
                if name and 0 < len(name) <= 50 and ' ' not in name:
                    MY_NAME = name; MY_ID = None; is_guest = True
                    update_my_info(MY_NAME, MY_P2P_PORT) # Need info before sending
                    login_data = {'type': 'guest_login','name': MY_NAME,'ip': MY_INFO.get('ip'),'p2p_port': MY_P2P_PORT}
                    potential_login_info = {'type': 'guest', 'name': MY_NAME}
                else: print("[ERROR] Invalid guest name (1-50 chars, no spaces).")
            elif cmd_lower.startswith('/login '):
                parts = cmd.split(' ', 2)
                name = parts[1].strip() if len(parts)>1 else None
                id_str = parts[2].strip() if len(parts)>2 else None
                if name and 0 < len(name) <= 50 and ' ' not in name and id_str and id_str.isdigit():
                     MY_NAME = name; MY_ID = id_str; is_guest = False
                     update_my_info(MY_NAME, MY_P2P_PORT) # Need info before sending
                     login_data = {'type': 'user_login','name': MY_NAME,'id': MY_ID,'ip': MY_INFO.get('ip'),'p2p_port': MY_P2P_PORT}
                     potential_login_info = {'type': 'user', 'name': MY_NAME, 'id': MY_ID}
                else: print("[ERROR] Invalid login format. Usage: /login <name> <id_number>")
            else: print("[ERROR] Unknown command. Use /guest or /login.")

            # --- Attempt Connection & Registration ---
            if login_data:
                try:
                    print(f"[CONNECTING] to Registry {host}:{port}...")
                    temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    temp_socket.settimeout(10.0)
                    temp_socket.connect((host, port))
                    print("[CONNECTED] Sending identification...");
                    login_data['is_invisible'] = is_invisible # Include current visibility
                    temp_socket.sendall((json.dumps(login_data) + "\n").encode('utf-8'))

                    print("[WAITING] for acknowledgement..."); buffer = ""; ack_received = False
                    temp_socket.settimeout(20.0) # Timeout for ack
                    while not ack_received:
                        try:
                            data = temp_socket.recv(BUFFER_SIZE)
                            if not data: print("[ERROR] Connection closed by server."); ack_received=True; break
                            buffer += data.decode('utf-8')
                            if '\n' in buffer:
                                resp_json, buffer = buffer.split('\n', 1); response = json.loads(resp_json)
                                r_type = response.get('type')
                                if r_type == 'login_ack' or r_type == 'guest_ack':
                                    print(f"[SUCCESS] {response.get('message')}"); server_ip = response.get('client_ip')
                                    if server_ip: update_my_info(MY_NAME, MY_P2P_PORT, ip=server_ip) # Update with server-verified IP
                                    ack_received=True
                                    # --- SUCCESS ---
                                    registry_socket = temp_socket # Assign to global socket
                                    registry_socket.settimeout(None) # Listener will set its own timeout
                                    initial_login_info = potential_login_info # Store successful login info
                                    return True # Exit function successfully
                                elif r_type == 'error':
                                    print(f"[FAILED] {response.get('message')}"); ack_received=True; break # Break inner loop only
                                else: print(f"[WARNING] Unexpected ack response: {r_type}")
                        except socket.timeout: print("[ERROR] Timeout waiting for server acknowledgement."); ack_received=True; break
                        except (socket.error, json.JSONDecodeError, UnicodeDecodeError) as e: print(f"[ERROR] Acknowledgement error: {e}"); ack_received=True; break

                    # If ack loop finished without success
                    if registry_socket is None: # Check if global socket was assigned
                        if temp_socket: temp_socket.close(); temp_socket = None
                        print("[INFO] Registration failed. Please try again."); continue # Go back to command prompt

                except socket.timeout: print(f"[ERROR] Connection to {host}:{port} timed out."); time.sleep(1); continue
                except socket.error as e: print(f"[ERROR] Registry connection error: {e}"); time.sleep(1); continue
                except Exception as e: print(f"[CRITICAL] Login error: {e}"); running=False; return False # Exit client on unexpected error

        except (EOFError, KeyboardInterrupt): print("\n[EXITING]"); running = False; break # Break outer loop
        except Exception as e: print(f"[CRITICAL] Input loop error: {e}"); running = False; break

    # Cleanup if loop exited due to error or Ctrl+C before success
    if temp_socket: 
        try: temp_socket.close()
        except: pass
    return False # Return failure

# --- Reconnect and Re-register ---
def reconnect_and_register(host, port):
    """Attempts to reconnect to the registry and re-register using stored info."""
    global registry_socket, MY_INFO, initial_login_info, running

    if not initial_login_info: print("[ERROR] Cannot reconnect: Initial login info missing."); return False

    # --- Prepare Login Data ---
    login_data = {}; login_type = initial_login_info.get('type'); name = initial_login_info.get('name'); user_id = initial_login_info.get('id')
    update_my_info(name, MY_P2P_PORT) # Refresh IP just in case
    if login_type == 'guest' and name:
         login_data = {'type': 'guest_login','name': name,'ip': MY_INFO.get('ip'),'p2p_port': MY_P2P_PORT}
    elif login_type == 'user' and name and user_id:
         login_data = {'type': 'user_login','name': name,'id': user_id,'ip': MY_INFO.get('ip'),'p2p_port': MY_P2P_PORT}
    else: print("[ERROR] Cannot reconnect: Invalid stored login info."); return False
    login_data['is_invisible'] = is_invisible # Send current visibility

    # --- Attempt Connection & Registration ---
    temp_socket = None
    try:
        print(f"[RECONNECTING] to Registry {host}:{port}...")
        temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM); temp_socket.settimeout(10.0)
        temp_socket.connect((host, port))
        print("[CONNECTED] Sending identification...");
        temp_socket.sendall((json.dumps(login_data) + "\n").encode('utf-8'))
        print("[WAITING] for acknowledgement..."); buffer = ""; ack_received = False
        temp_socket.settimeout(20.0)
        while not ack_received:
            try:
                data = temp_socket.recv(BUFFER_SIZE)
                if not data: print("[ERROR] Connection closed by server during reconnect."); break
                buffer += data.decode('utf-8')
                if '\n' in buffer:
                    resp_json, buffer = buffer.split('\n', 1); response = json.loads(resp_json)
                    r_type = response.get('type')
                    if r_type == 'login_ack' or r_type == 'guest_ack':
                        print(f"[RECONNECT SUCCESS] {response.get('message')}"); server_ip = response.get('client_ip')
                        if server_ip: update_my_info(name, MY_P2P_PORT, ip=server_ip) # Update IP
                        # --- SUCCESS ---
                        registry_socket = temp_socket # Assign global socket
                        registry_socket.settimeout(None) # Listener will set its own
                        return True
                    elif r_type == 'error': print(f"[RECONNECT FAILED] {response.get('message')}"); break
                    else: print(f"[WARNING] Unexpected ack response during reconnect: {r_type}")
            except socket.timeout: print("[ERROR] Timeout waiting for reconnect ack."); break
            except (socket.error, json.JSONDecodeError, UnicodeDecodeError) as e: print(f"[ERROR] Reconnect ack error: {e}"); break
    except socket.timeout: print(f"[ERROR] Connection timed out during reconnect.");
    except socket.error as e: print(f"[ERROR] Reconnect connection error: {e}");
    except Exception as e: print(f"[CRITICAL] Reconnect error: {e}");
    # --- Failure Cleanup ---
    if temp_socket: 
        try: temp_socket.close()
        except: pass
    registry_socket = None # Ensure global socket is None on failure
    return False

# --- Main Client Function ---
def start_p2p_client():
    global running, MY_NAME, MY_INFO, is_offline, initial_login_info, is_guest, MY_ID
    global registry_socket, p2p_listener_thread, registry_listener_thread
    global stop_p2p_listener_event, stop_registry_listener_event

    # --- Initial Login Attempt ---
    login_successful = login_or_guest(REGISTRY_HOST, REGISTRY_PORT)
    if not login_successful:
        print("[EXITING] Initial login failed."); running = False; time.sleep(0.5); return

    # --- Start Initial Listeners ---
    is_offline = False # Start online
    stop_p2p_listener_event = threading.Event()
    stop_registry_listener_event = threading.Event()

    print(f"[INFO] Starting P2P listener on {LISTEN_HOST}:{MY_P2P_PORT}...")
    p2p_listener_thread = threading.Thread(target=listen_for_peers, args=(LISTEN_HOST, MY_P2P_PORT, stop_p2p_listener_event), daemon=True)
    p2p_listener_thread.start()
    time.sleep(0.1)

    print("[INFO] Starting Registry listener...")
    if registry_socket:
        registry_listener_thread = threading.Thread(target=listen_to_registry, args=(registry_socket, stop_registry_listener_event), daemon=True)
        registry_listener_thread.start()
    else:
        print("[CRITICAL] Registry socket unavailable after login. Forcing offline state.")
        is_offline = True # Force offline if socket somehow failed post-login

    # --- Logged In Welcome Message ---
    print("-" * 30 + f"\n--- Welcome, {'Guest ' if is_guest else ''}{MY_NAME} " + (f"(ID: {MY_ID})" if not is_guest else "") + " ---")
    print(f"Your Info: {MY_INFO}")
    print("--- Commands: /list /myinfo /msg /create /join /list_channels /my_channels /members /history /quit /invisible /offline /online ---")
    print("-" * 30)

    # --- Main Command Loop ---
    while running:
        try:
            prompt_state = "OFFLINE" if is_offline else MY_NAME
            prompt = f"({prompt_state})> "
            cmd = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            cmd = "/quit" # Treat interrupt as quit

        if not running: break
        if not cmd: continue
        cmd_lower = cmd.lower()

        # --- Always Allow Quit ---
        if cmd_lower == '/quit':
            print("[COMMAND] Quit requested.")
            if not is_offline and registry_socket: # Try to notify if online
                 try:
                     update_msg = {'type': 'update_offline_status', 'is_offline': True} # Notify leaving
                     registry_socket.sendall((json.dumps(update_msg) + "\n").encode('utf-8'))
                 except Exception: pass
            stop_p2p_listener_event.set()
            stop_registry_listener_event.set()
            if registry_socket: 
                try: registry_socket.close()
                except Exception: 
                    pass
                    registry_socket = None
            running = False
            print("[SHUTDOWN] Exiting...")
            break

        # --- Handle Offline State ---
        if is_offline:
            if cmd_lower == '/online':
                print("[COMMAND] Attempting to go online...")
                if reconnect_and_register(REGISTRY_HOST, REGISTRY_PORT):
                    print("[STATUS] Reconnected and registered successfully.")
                    is_offline = False
                    # Send online status update
                    try:
                         update_msg = {'type': 'update_offline_status', 'is_offline': False}
                         if registry_socket: registry_socket.sendall((json.dumps(update_msg) + "\n").encode('utf-8'))
                         else: print("[ERROR] Reconnected but socket missing? Cannot send online status.")
                    except Exception as e: print(f"[ERROR] Failed to send online status update: {e}")
                    # Restart listeners with NEW events
                    stop_p2p_listener_event = threading.Event()
                    stop_registry_listener_event = threading.Event()
                    print(f"[INFO] Restarting P2P listener...")
                    p2p_listener_thread = threading.Thread(target=listen_for_peers, args=(LISTEN_HOST, MY_P2P_PORT, stop_p2p_listener_event), daemon=True)
                    p2p_listener_thread.start()
                    time.sleep(0.1)
                    print("[INFO] Restarting Registry listener...")
                    if registry_socket:
                         registry_listener_thread = threading.Thread(target=listen_to_registry, args=(registry_socket, stop_registry_listener_event), daemon=True)
                         registry_listener_thread.start()
                    else: print("[ERROR] Cannot restart Registry listener: Socket missing.")
                else: print("[STATUS] Failed to go online. Please check connection or registry server.")
            # --- Allowed Offline Commands ---
            elif cmd_lower == '/myinfo': print(f"[YOUR INFO]: {MY_INFO} | Status: {'Guest' if is_guest else f'User (ID: {MY_ID})'} | State: OFFLINE")
            elif cmd_lower == '/my_channels':
                print("[YOUR CHANNELS (Local View)]:")
                with my_channels_lock: channels = sorted(my_channels.items())
                if channels:
                    for name, data in channels:
                        role = "Owner" if data.get('owner') == MY_NAME else "Member"; count = len(data.get('members',{})) if role=="Owner" else "?"; owner=data.get('owner','?')
                        print(f"  - {name} ({role}, Owner: {owner}, {count} members)")
                else: print("  (Not in any channels locally)")
            # elif cmd_lower.startswith('/history '):
            #     parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
            #     if name:
            #         is_owner = False; 
            #         with my_channels_lock: 
            #             is_owner = name in my_channels and my_channels[name].get('owner') == MY_NAME
            #         if is_owner:
            #             print(f"[HISTORY for '{name}'] (Stored locally by owner):")
            #             log_entries = []; 
            #             with channel_logs_lock: 
            #                 log_entries = channel_message_logs.get(name, []).copy()
            #             if log_entries:
            #                  for entry in log_entries:
            #                      ts=entry.get('timestamp','N/A'); sender=entry.get('sender','?'); content=entry.get('content','')
            #                      try: ts_obj=datetime.datetime.fromisoformat(ts); time_str=ts_obj.strftime('%Y-%m-%d %H:%M:%S')
            #                      except: time_str=ts
            #                      print(f"  [{time_str}] {sender}: {content}")
            #             else: print("  (No messages logged locally)")
            #         else: print(f"[CMD ERROR] Can only view history for channels you own.")
            #     else: print("[CMD ERROR] Usage: /history <channel_name>")
            # elif cmd_lower.startswith('/members '):
            #      parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
            #      if name:
            #          with my_channels_lock: channel_data = my_channels.get(name)
            #          if channel_data:
            #              print(f"[MEMBERS of '{name}'] (Local View):"); members = sorted(channel_data.get('members', {}).keys())
            #              if members: [print(f"  - {m} {'(Owner)' if m == channel_data.get('owner') else ''} {'(You)' if m == MY_NAME else ''}") for m in members]
            #              else: print("  (No members known locally)")
            #          else: print(f"[CMD ERROR] Not part of channel '{name}' locally.")
            #      else: print("[CMD ERROR] Usage: /members <channel_name>")
            elif cmd_lower.startswith('/msg '): # Handles local owner "send"
                 parts = cmd.split(' ', 2)
                 if len(parts) == 3:
                     channel_name = parts[1].strip(); msg_content = parts[2]
                     if channel_name and msg_content: send_channel_msg(channel_name, msg_content, None) # Socket is None offline
                     else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")
                 else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")
            elif cmd_lower == '/invisible': # Allow setting state offline
                 is_invisible = not is_invisible
                 status = "invisible" if is_invisible else "visible"; print(f"[STATUS] Visibility set to {status}. Will apply when next online.")
            elif cmd_lower == '/quit': pass # Handled above
            else: print("[INFO] You are offline. Available: /online, /quit, /myinfo, /my_channels, /members, /history, /msg (owner only), /invisible.")
            continue # Skip online command checks

        # --- Online State Commands ---
        elif cmd_lower == '/offline':
            if is_offline: continue # Already offline
            print("[COMMAND] Going offline...")
            try: # Notify server
                update_msg = {'type': 'update_offline_status', 'is_offline': True}
                if registry_socket: registry_socket.sendall((json.dumps(update_msg) + "\n").encode('utf-8')); print("[STATUS] Offline status notification sent.")
                else: print("[WARNING] Cannot send offline status: Not connected.")
            except Exception as e: print(f"[WARNING] Failed to send offline status notification: {e}")
            stop_p2p_listener_event.set(); stop_registry_listener_event.set() # Signal listeners
            if registry_socket: 
                print("[CLOSING] Registry connection...")
                try: registry_socket.close()
                except Exception: pass
                registry_socket = None
            is_offline = True; print("[STATUS] You are now offline. Network listeners stopped.")

        elif cmd_lower == '/invisible':
            is_invisible = not is_invisible; status = "invisible" if is_invisible else "visible"
            print(f"[STATUS] You are now {status}")
            try: # Notify server
                update_msg = {'type': 'update_visibility', 'is_invisible': is_invisible}
                if registry_socket: registry_socket.sendall((json.dumps(update_msg) + "\n").encode('utf-8'))
                else: print("[ERROR] Cannot update visibility: Offline.")
            except Exception as e: print(f"[ERROR] Failed to update visibility status: {e}"); is_invisible = not is_invisible # Revert

        elif cmd_lower == '/list':
            print("[CMD] Requesting peer list...")
            try:
                msg = {'type': 'list', 'peer_id': f"user_{MY_P2P_PORT}" if not is_guest else f"guest_{MY_P2P_PORT}"}
                if registry_socket: registry_socket.sendall((json.dumps(msg) + "\n").encode('utf-8'))
                else: print("[ERROR] Cannot list peers: Offline.")
            except Exception as e: print(f"[ERROR] Failed to send list request: {e}")

        elif cmd_lower == '/myinfo': print(f"[YOUR INFO]: {MY_INFO} | Status: {'Guest' if is_guest else f'User (ID: {MY_ID})'} | State: ONLINE")

        elif cmd_lower.startswith('/msg '):
            parts = cmd.split(' ', 2)
            if len(parts) == 3:
                channel_name = parts[1].strip(); msg_content = parts[2]
                if channel_name and msg_content: send_channel_msg(channel_name, msg_content, registry_socket) # Pass socket
                else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")
            else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")

        elif cmd_lower.startswith('/create '):
            parts = cmd.split(' ', 2)
            if len(parts) == 3:
                name = parts[1].strip(); channel_type = parts[2].strip()
                if name and channel_type: create_channel(registry_socket, name, channel_type) # Function handles socket check
                else: print("[CMD ERROR] Usage: /create <channel_name> <public/private>")
            else: print("[CMD ERROR] Usage: /create <channel_name> <public/private>")

        elif cmd_lower.startswith('/join '):
             parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
             if name: join_channel(name) # Function handles P2P send which requires online
             else: print("[CMD ERROR] Usage: /join <channel_name>")

        elif cmd_lower == '/list_channels':
             print("[CMD] Requesting channel list...")
             try:
                  if registry_socket: registry_socket.sendall((json.dumps({'type': 'request_channel_list'}) + "\n").encode('utf-8'))
                  else: print("[ERROR] Cannot list channels: Offline.")
             except Exception as e: print(f"[ERROR] Sending request: {e}")

        elif cmd_lower == '/my_channels': # Same as offline version
            print("[YOUR CHANNELS (Local View)]:")
            with my_channels_lock: channels = sorted(my_channels.items())
            if channels:
                for name, data in channels:
                    role = "Owner" if data.get('owner') == MY_NAME else "Member"; count = len(data.get('members',{})) if role=="Owner" else "?"; owner=data.get('owner','?')
                    print(f"  - {name} ({role}, Owner: {owner}, {count} members)")
            else: print("  (Not in any channels locally)")

        elif cmd_lower.startswith('/members '): # Same as offline version
             parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
             if name:
                 with my_channels_lock: channel_data = my_channels.get(name)
                 if channel_data:
                     print(f"[MEMBERS of '{name}'] (Local View):"); members = sorted(channel_data.get('members', {}).keys())
                     if members: [print(f"  - {m} {'(Owner)' if m == channel_data.get('owner') else ''} {'(You)' if m == MY_NAME else ''}") for m in members]
                     else: print("  (No members known locally)")
                 else: print(f"[CMD ERROR] Not part of channel '{name}' locally.")
             else: print("[CMD ERROR] Usage: /members <channel_name>")

        # elif cmd_lower.startswith('/history '): # Same as offline version
        #     parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
        #     if name:
        #         is_owner = False
        #         with my_channels_lock: is_owner = name in my_channels and my_channels[name].get('owner') == MY_NAME
        #         if is_owner:
        #             print(f"[HISTORY for '{name}'] (Stored locally by owner):")
        #             log_entries = []
        #             with channel_logs_lock: 
        #                 log_entries = channel_message_logs.get(name, []).copy()
        #             if log_entries:
        #                 for entry in log_entries:
        #                     ts=entry.get('timestamp','N/A'); sender=entry.get('sender','?'); content=entry.get('content','')
        #                     try: ts_obj=datetime.datetime.fromisoformat(ts); time_str=ts_obj.strftime('%Y-%m-%d %H:%M:%S')
        #                     except: time_str=ts
        #                     print(f"  [{time_str}] {sender}: {content}")
        #             else: print("  (No messages logged locally)")
        #         else: print(f"[CMD ERROR] Can only view history for channels you own.")
        #     else: print("[CMD ERROR] Usage: /history <channel_name>")

        else:
            print("[CMD ERROR] Unknown command.")


    # --- Cleanup (Only reached on /quit) ---
    if not running: # Should be true if exiting via /quit
        # Threads are daemons and should exit, or were signalled by events
        print("[INFO] Waiting briefly for threads...");
        time.sleep(0.5)
        print("[CLOSED] Client shut down complete.")


# --- Entry Point ---
if __name__ == "__main__":
    start_p2p_client()