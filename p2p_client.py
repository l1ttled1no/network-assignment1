# --- p2p_client.py ---
import socket
import threading
import sys
import time
import json
import random
# import ipaddress # Not strictly needed in this version
import datetime
from datetime import timezone

# --- Configuration ---
REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 9999
LISTEN_HOST = '0.0.0.0'
MY_P2P_PORT = random.randint(10000, 60000)
BUFFER_SIZE = 4096

# --- Shared State ---
peer_list_lock = threading.Lock()
known_peers = {}

my_channels_lock = threading.Lock()
my_channels = {} # { channel_name: {'owner': name, 'members': {name: info}} }

available_channels_info_lock = threading.Lock()
available_channels_info = {} # { channel_name: {'owner_name':.., 'owner_ip':.., 'owner_p2p_port':..} }

channel_logs_lock = threading.Lock()
# Local message logs ONLY for channels THIS client OWNS
channel_message_logs = {} # { channel_name: [{'timestamp':.., 'sender':.., 'content':..}] }

# --- Offline Message Queue ---
offline_queue_lock = threading.Lock()
# Stores messages typed while offline by ANYONE (owner or member)
# Format: [{'channel_name': str, 'content': str, 'timestamp': iso_str}, ...]
offline_message_queue = []

# --- Application Control ---
running = True
is_offline = False

# --- Identity and Info ---
MY_NAME = None
MY_ID = None
is_guest = None
MY_INFO = {}

initial_login_info = {} # For reconnect

# --- Network & Thread Management ---
registry_socket = None
p2p_listener_thread = None
registry_listener_thread = None
stop_p2p_listener_event = threading.Event()
stop_registry_listener_event = threading.Event()

# --- Visibility ---
is_invisible = False

# --- Helper Functions (get_local_ip, update_my_info, get_peer_name_by_address, get_peer_info_by_name) ---
# (Keep these functions as they were in the previous 'final code' version)
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
    current_ip = ip if ip else get_local_ip()
    new_info = {'ip': current_ip, 'p2p_port': p2p_port, 'name': name}
    if MY_INFO != new_info:
        MY_INFO = new_info
        source = "(from server)" if ip else "(determined locally)"
        print(f"[INFO] Updated own info {source}: {MY_INFO}")

def get_peer_name_by_address(ip, port):
    """Looks up peer name from known_peers or returns IP:Port."""
    if MY_INFO.get('ip') == ip and MY_INFO.get('p2p_port') == port: return f"{MY_NAME} (Self)"
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('ip') == ip and info.get('p2p_port') == port: return info.get('name', f"{ip}:{port}")
    with my_channels_lock:
        for channel in my_channels.values():
            for member_name, member_info in channel.get('members', {}).items():
                if member_info.get('ip') == ip and member_info.get('p2p_port') == port: return member_name
    return f"{ip}:{port}"

def get_peer_info_by_name(name):
    """Looks up peer IP/Port from known_peers or local channels."""
    if name == MY_NAME: return MY_INFO
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('name') == name: return info
    with my_channels_lock:
        for channel in my_channels.values():
            member_info = channel.get('members', {}).get(name)
            if member_info: return member_info
    return None

# --- P2P Listener (listen_for_peers) ---
# (Keep this function as it was in the previous 'final code' version, accepting stop_event)
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
        while running and not stop_event.is_set():
            try:
                peer_socket, peer_address = p2p_server_socket.accept()
                peer_thread = threading.Thread(target=handle_peer_connection, args=(peer_socket, peer_address), daemon=True)
                peer_thread.start()
            except socket.timeout: continue
            except OSError as e:
                 if not stop_event.is_set() and running: print(f"[P2P LISTENER ERROR] Accept error: {e}")
                 break
            except Exception as e:
                if running and not stop_event.is_set(): print(f"[P2P LISTENER CRITICAL ERROR] Unexpected error: {e}")
                break
    except OSError as e:
        if running and not stop_event.is_set(): print(f"[P2P LISTENER BIND ERROR] Could not bind P2P listener to {host}:{port} - {e}.")
    except Exception as e:
        if running and not stop_event.is_set(): print(f"[P2P LISTENER CRITICAL] Other error: {e}")
    finally:
        print("[P2P LISTENING] Shutting down P2P listener.")
        if p2p_server_socket:
            try: p2p_server_socket.close()
            except Exception as e: print(f"[P2P LISTENER WARN] Error closing listener socket: {e}")


# --- P2P Connection Handler (handle_peer_connection) ---
# (Keep this function as it was in the previous 'final code' version)
def handle_peer_connection(peer_socket, peer_address):
    """Handles incoming P2P connection and processes messages."""
    actual_peer_name = get_peer_name_by_address(peer_address[0], peer_address[1])
    prompt = f"({ 'OFFLINE' if is_offline else MY_NAME })> "
    buffer = ""
    try:
        while running:
            try:
                data = peer_socket.recv(BUFFER_SIZE)
                if not data: break
            except ConnectionResetError: break
            except socket.error: break
            try: buffer += data.decode('utf-8')
            except UnicodeDecodeError: print(f"\n[P2P ERROR] Non-UTF8 from {actual_peer_name}."); buffer = ""; continue

            while '\n' in buffer and running:
                message_json, buffer = buffer.split('\n', 1)
                if not message_json.strip(): continue
                try:
                    message = json.loads(message_json)
                    msg_type = message.get('type')
                    current_prompt = f"({ 'OFFLINE' if is_offline else MY_NAME })> "

                    # --- Join Request ---
                    if msg_type == 'request_join_channel':
                        req_channel=message.get('channel_name'); req_name=message.get('requester_name'); req_ip=message.get('requester_ip'); req_port=message.get('requester_p2p_port')
                        if req_name: actual_peer_name = req_name
                        print(f"\r{' ' * 60}\r[P2P RECV] Join request for '{req_channel}' from '{req_name}'\n{current_prompt}", end="", flush=True)
                        if not (req_channel and req_name and req_ip and req_port): print(f"[P2P WARNING] Invalid join request from {actual_peer_name}"); continue
                        with my_channels_lock:
                            if req_channel in my_channels and my_channels[req_channel].get('owner') == MY_NAME:
                                channel_data=my_channels[req_channel]; members_dict=channel_data.setdefault('members',{})
                                requester_info={'name':req_name,'ip':req_ip,'p2p_port':req_port}
                                if req_name not in members_dict: print(f"[CHANNEL] Adding '{req_name}' to '{req_channel}'.")
                                else: print(f"[CHANNEL INFO] '{req_name}' re-requested join. Updating.")
                                members_dict[req_name] = requester_info
                                accept_response={'type':'join_channel_accepted','channel_name':req_channel,'owner_name':MY_NAME,'members':members_dict}
                                threading.Thread(target=send_p2p_message, args=(req_name, json.dumps(accept_response) + '\n'), daemon=True).start()
                                print(f"[P2P >>] Sent join acceptance for '{req_channel}' to '{req_name}'.")
                            elif req_channel in my_channels: print(f"[P2P WARNING] Join request for '{req_channel}', but not owner ({my_channels[req_channel].get('owner','?')}). Ignoring.")
                            else: print(f"[P2P WARNING] Join request for unknown local channel '{req_channel}'. Ignoring.")

                    # --- Join Acceptance ---
                    elif msg_type == 'join_channel_accepted':
                        accepted_channel=message.get('channel_name'); accepted_owner=message.get('owner_name'); accepted_members=message.get('members',{})
                        print(f"\r{' ' * 60}\r[P2P RECV] Joined channel '{accepted_channel}' (Owner: {accepted_owner})!")
                        print(f"[CHANNEL] Members: {list(accepted_members.keys())}\n{current_prompt}", end="", flush=True)
                        with my_channels_lock: my_channels[accepted_channel] = {'owner': accepted_owner, 'members': accepted_members}

                    # --- Join Error ---
                    elif msg_type == 'join_error':
                        failed_channel=message.get('channel_name','N/A'); reason=message.get('reason','Unknown')
                        print(f"\r{' ' * 60}\r[P2P RECV] Join Failed for '{failed_channel}': {reason}\n{current_prompt}", end="", flush=True)

                    # --- Channel Message ---
                    elif msg_type == 'channel_message':
                        channel=message.get('channel_name'); sender=message.get('sender'); content=message.get('content'); timestamp=message.get('timestamp','N/A')
                        if channel and sender and content:
                            with my_channels_lock: is_member = channel in my_channels
                            if is_member:
                                try: ts_obj=datetime.datetime.fromisoformat(timestamp); time_str=ts_obj.strftime('%H:%M:%S')
                                except: time_str="time?"
                                display_sender = f"{sender} (You)" if sender == MY_NAME else sender
                                print(f"\r{' ' * 60}\r[{channel} @ {time_str}] {display_sender}: {content}\n{current_prompt}", end="", flush=True)

                    # --- Forward Request ---
                    elif msg_type == 'forward_to_owner':
                        channel=message.get('channel_name'); original_sender=message.get('sender'); msg_content=message.get('content'); timestamp=message.get('timestamp', datetime.datetime.now(timezone.utc).isoformat())
                        if not (channel and original_sender and msg_content is not None): print(f"\n[P2P WARNING] Invalid 'forward_to_owner'."); continue
                        is_owner=False; members_to_forward={}
                        with my_channels_lock:
                            if channel in my_channels and my_channels[channel].get('owner') == MY_NAME:
                                is_owner=True; members_to_forward=my_channels[channel].get('members',{}).copy()
                        if is_owner:
                            with channel_logs_lock: 
                                log_entry={'timestamp':timestamp,'sender':original_sender,'content':msg_content}; 
                                channel_message_logs.setdefault(channel,[]).append(log_entry)
                            try: ts_obj=datetime.datetime.fromisoformat(timestamp); time_str=ts_obj.strftime('%H:%M:%S')
                            except: time_str="time?"
                            print(f"\r{' ' * 60}\r[{channel} @ {time_str}] {original_sender}: {msg_content}\n{current_prompt}", end="", flush=True)
                            forward_payload={'type':'channel_message','channel_name':channel,'sender':original_sender,'content':msg_content,'timestamp':timestamp}
                            forward_json=json.dumps(forward_payload) + "\n"
                            for member_name in members_to_forward:
                                if member_name != MY_NAME and member_name != original_sender:
                                    threading.Thread(target=send_p2p_message, args=(member_name, forward_json), daemon=True).start()
                            
                            # Also send that message to host
                            registry_socket.sendall(forward_json.encode('utf-8'))

                    else: print(f"\r{' ' * 60}\r[P2P UNHANDLED from {actual_peer_name}]: Type: {msg_type}\n{current_prompt}", end="", flush=True)
                except json.JSONDecodeError: print(f"\n[P2P ERROR] Invalid JSON from {actual_peer_name}: {message_json}")
                except Exception as e: print(f"\n[P2P ERROR] Processing message from {actual_peer_name}: {e}"); import traceback; traceback.print_exc()
    except Exception as e:
        if running: print(f"\n[P2P HANDLER CRITICAL] Error with {actual_peer_name}: {e}")
    finally:
        try: peer_socket.shutdown(socket.SHUT_RDWR)
        except: pass
        try: peer_socket.close()
        except: pass


# --- Registry Listener (listen_to_registry) ---
# (Keep this function as it was in the previous 'final code' version, accepting stop_event)
def listen_to_registry(reg_socket, stop_event):
    """Listens for updates from the registry and stops when event is set."""
    global running, known_peers, MY_INFO, my_channels, available_channels_info
    if not reg_socket: print("[REGISTRY LISTENER] Error: No registry socket provided."); return
    buffer = ""
    try:
        reg_socket.settimeout(1.0)
        while running and not stop_event.is_set():
            try:
                data = reg_socket.recv(BUFFER_SIZE)
                if not data:
                    if running and not stop_event.is_set(): print("\n[CONNECTION LOST] Registry server closed connection.")
                    break
                try: buffer += data.decode('utf-8')
                except UnicodeDecodeError: print("\n[REGISTRY ERROR] Invalid UTF-8 data."); buffer=""; continue

                while '\n' in buffer:
                    if not running or stop_event.is_set(): break
                    message_json, buffer = buffer.split('\n', 1)
                    if not message_json.strip(): continue
                    current_prompt = f"({ 'OFFLINE' if is_offline else MY_NAME })> "
                    try:
                        message = json.loads(message_json)
                        msg_type = message.get('type')

                        if msg_type == 'peer_list':
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
                        elif msg_type == 'peer_joined':
                            peer_id=message.get('id'); info=message.get('peer_info'); name=info.get('name','Unk') if info else 'Unk'
                            if peer_id and info and name != MY_NAME and not info.get('is_invisible',False):
                                with peer_list_lock: known_peers[peer_id] = info
                                print(f"\r{' ' * 60}\r[REGISTRY] Peer joined: {name}\n{current_prompt}", end="", flush=True)
                        elif msg_type == 'peer_left':
                            peer_id = message.get('id')
                            if peer_id:
                                removed_name="Unk"; 
                                with peer_list_lock:
                                    if peer_id in known_peers: removed_name = known_peers.pop(peer_id).get('name', 'Unk')
                                if removed_name != "Unk": print(f"\r{' ' * 60}\r[REGISTRY] Peer left: {removed_name}\n{current_prompt}", end="", flush=True)
                        elif msg_type == 'channel_created':
                            c_name=message.get('channel_name'); c_owner=message.get('owner_name')
                            print(f"\r{' ' * 60}\r[REGISTRY] Channel '{c_name}' creation confirmed.\n{current_prompt}", end="", flush=True)
                            if c_owner == MY_NAME:
                                with my_channels_lock:
                                    if c_name not in my_channels:
                                         if not MY_INFO: update_my_info(MY_NAME, MY_P2P_PORT)
                                         my_channels[c_name] = {'owner': MY_NAME, 'members': {MY_NAME: MY_INFO.copy()}}
                        elif msg_type == 'channel_error':
                             error_msg=message.get('message','Unk'); c_name=message.get('channel_name','N/A')
                             print(f"\r{' ' * 60}\r[REGISTRY ERROR] Channel '{c_name}': {error_msg}\n{current_prompt}", end="", flush=True)
                        elif msg_type == 'new_channel_available':
                             c_name=message.get('channel_name'); c_owner=message.get('owner_name'); c_ip=message.get('owner_ip'); c_port=message.get('owner_p2p_port')
                             print(f"\r{' ' * 60}\r[REGISTRY] New channel available: '{c_name}' (Owner: {c_owner})\n{current_prompt}", end="", flush=True)
                             if c_name and c_owner and c_ip and c_port:
                                  with available_channels_info_lock: available_channels_info[c_name] = {'owner_name':c_owner,'owner_ip':c_ip,'owner_p2p_port':c_port}
                        elif msg_type == 'channel_list':
                            print(f"\r{' ' * 60}\r[REGISTRY] Received list of all channels:")
                            channels_data = message.get('channels', {})
                            with available_channels_info_lock:
                                available_channels_info.clear()
                                if channels_data:
                                    for name, info in sorted(channels_data.items()):
                                        owner=info.get('owner_name','N/A'); contact=f"{info.get('owner_ip','N/A')}:{info.get('owner_p2p_port','N/A')}"
                                        print(f"  - {name} (Owner: {owner} @ {contact})")
                                        available_channels_info[name] = info
                                else: print("  (No channels currently registered)")
                            print(f"{current_prompt}", end="", flush=True)
                        elif msg_type == 'error':
                            error_msg = message.get('message', 'Unknown server error')
                            print(f"\r{' ' * 60}\r[SERVER ERROR] {error_msg}\n{current_prompt}", end="", flush=True)
                        elif msg_type == 'server_shutdown':
                            print(f"\r{' ' * 60}\r[REGISTRY] {message.get('message', 'Server Shutting Down')}\n{current_prompt}", end="", flush=True)
                            running = False; break
                        
                        elif msg_type == 'sync_reply':
                            channel_name = message.get('channel_name')
                            # Rename for clarity - it's a list of messages
                            received_messages = message.get('messages')
                            return_status = message.get('status')

                            # Basic validation
                            if not channel_name or received_messages is None or return_status is None:
                                print(f"[SYNC REPLY ERROR] Received incomplete sync_reply: {message}")
                                # Handle error appropriately, maybe continue/return
                                continue # Or return depending on context

                            print(f"[SYNC REPLY] Received reply for channel '{channel_name}', status: '{return_status}', messages: {len(received_messages)}")

                            if return_status == 'sync_point_not_found':
                                # This means the server didn't find the timestamp the client sent.
                                # The server might have sent no messages or all messages depending on its logic.
                                # If the server sent no messages ('messages': []), this print is accurate.
                                # If the server sent ALL messages, the logic below will handle adding them.
                                if not received_messages:
                                     print(f"[SYNCHRONIZATION] Server didn't find sync point for '{channel_name}', no messages provided. Assuming up-to-date or channel empty.")
                                else:
                                     print(f"[SYNCHRONIZATION] Server didn't find sync point for '{channel_name}', receiving full log ({len(received_messages)} messages).")
                                     # Fall through to the adding logic below

                            elif return_status == 'sync_point_found':
                                if not received_messages:
                                     # This means the client had the latest message already
                                     print(f"[SYNCHRONIZATION] Sync point found for '{channel_name}', no newer messages received. Already up-to-date.")
                                     # No need to add anything
                                     continue # Or return
                                else:
                                    print(f"[SYNCHRONIZATION] Sync point found for '{channel_name}', receiving {len(received_messages)} new or subsequent messages.")
                                     # Fall through to the adding logic below
                            
                            # --- Add received messages (if any) ---
                            # This block executes if status was 'sync_point_found' and messages were received,
                            # OR if status was 'sync_point_not_found' and the server sent messages anyway (e.g., the full log)
                            if received_messages: # Check if the list is not empty
                                with channel_logs_lock:
                                    # Get the specific log list for this channel, creating it if it doesn't exist
                                    log_for_channel = channel_message_logs.setdefault(channel_name, [])

                                    # *** Use extend() to add items from received_messages list ***
                                    log_for_channel.extend(received_messages)



                                    print(f"[SYNCHRONIZATION] Updated log for '{channel_name}'. New total size: {len(log_for_channel)}")
                                    print(f"Last added messages: {received_messages}") # Optional: Print the messages just added

                            elif return_status != 'sync_point_found' and return_status != 'sync_point_not_found':
                                # Handle unexpected status from server if necessary
                                print(f"[SYNC REPLY WARNING] Received unknown status '{return_status}' for channel '{channel_name}'")
                
                        elif msg_type == 'notify_new_msg':
                            # --- Extract data ---
                            channel_name = message.get('channel_name')
                            sender = message.get('sender')
                            content = message.get('content') # Renaming back for clarity
                            timestamp_str = message.get('timestamp') # Assume ISO format string

                            # --- Basic Validation ---
                            if not channel_name or sender is None or content is None or timestamp_str is None:
                                print(f"\r{' ' * 60}\r[NOTIFY ERROR] Incomplete 'notify_new_msg': {message}\n{current_prompt}", end="", flush=True)
                                continue # Skip this message

                            # --- Check Membership CORRECTLY ---
                            is_member = False
                            with my_channels_lock:
                                # Check if this specific channel_name exists in the user's channels
                                if channel_name in my_channels:
                                    is_member = True
                            # Lock is released here

                            # --- Display Message IF Member ---
                            if is_member:
                                # Format timestamp nicely for display
                                try:
                                    ts_obj = datetime.datetime.fromisoformat(timestamp_str)
                                    time_str = ts_obj.strftime('%H:%M:%S')
                                except (ValueError, TypeError):
                                    time_str = "time?" # Fallback


                                # Print the formatted message, clearing the input line first
                                print(f"\r{' ' * 60}\r[{channel_name} @ {time_str}] {sender}: {content}")

                                # After printing the message, reprint the input prompt
                                print(f"{current_prompt}", end="", flush=True)

                            else:
                                # Not a member of this channel, ignore the notification silently
                                pass # Do nothing as requested



                        else: print(f"\r{' ' * 60}\r[REGISTRY UNHANDLED] Type: {msg_type}\n{current_prompt}", end="", flush=True)
                    except json.JSONDecodeError: print(f"\r{' ' * 60}\r[REGISTRY ERROR] Invalid JSON received\n{current_prompt}", end="", flush=True)
                    except Exception as e: print(f"\n[ERROR] Processing registry msg: {e}\nMsg: {message_json}\n{current_prompt}", end="", flush=True)
                if not running or stop_event.is_set(): break
            except socket.timeout: continue
            except (ConnectionResetError, ConnectionAbortedError, OSError) as e:
                if running and not stop_event.is_set(): print(f"\n[REGISTRY LISTENER ERROR] Connection error: {e}")
                break
            except Exception as e:
                if running and not stop_event.is_set(): print(f"\n[ERROR] Unexpected error receiving from registry: {e}")
                break
    finally: print("[THREAD] Registry listener finished.")


# --- P2P Message Sending (send_p2p_message) ---
def send_p2p_message(target_name, message_json_with_newline):
    """Sends a pre-formatted JSON message string directly to a peer's P2P listener,
       checking local offline status and target's *registry-reported* offline status."""

    # 1. Check if THIS client is offline
    if is_offline:
        print(f"[P2P SEND ERROR] Cannot send to '{target_name}': You are currently offline.")
        return False

    # 2. Find the target peer's connection info (IP/Port).
    #    This might come from known_peers OR my_channels for connection purposes.
    target_info = get_peer_info_by_name(target_name)
    if not target_info:
        print(f"[P2P SEND ERROR] Peer '{target_name}' not found in known peers or local channels.")
        return False

    # 3. *** NEW: Explicitly check registry status in known_peers ***
    target_is_offline_in_registry = False
    found_in_registry = False
    with peer_list_lock: # Need lock to safely access known_peers
        # Iterate through known_peers (populated by registry) to find by name
        for peer_id, registry_info in known_peers.items():
            if registry_info.get('name') == target_name:
                found_in_registry = True
                # Check the offline status specifically from the registry data
                target_is_offline_in_registry = registry_info.get('is_offline', False)
                break # Found the peer in the registry, no need to check further

    # 4. If found in registry and marked offline, stop the send attempt
    if found_in_registry and target_is_offline_in_registry:
        print(f"[P2P SEND INFO] Cannot send to '{target_name}': Peer is reported as offline by the registry in 'known_peers'.")
        return False # Message not sent

    # 5. If not found in registry, or found and reported online, proceed
    #    (We still use target_info from step 2 for the IP/Port, which might be
    #     from my_channels if the peer wasn't in known_peers yet)
    ip, port = target_info.get('ip'), target_info.get('p2p_port')
    if not ip or not port:
        print(f"[P2P SEND ERROR] Peer '{target_name}' incomplete info (IP/Port missing).")
        return False

    # 6. Attempt P2P connection and send
    p2p_send_socket = None
    try:
        # Optionally log if proceeding because registry status is okay (or unknown)
        if found_in_registry:
            print(f"[P2P SEND >>] Attempting connection to '{target_name}' (registry status: online) at {ip}:{port}")
            
            p2p_send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            p2p_send_socket.settimeout(5.0)
            p2p_send_socket.connect((ip, port))
            p2p_send_socket.sendall(message_json_with_newline.encode('utf-8'))
            print(f"[P2P SEND OK] Message sent successfully to '{target_name}'.")
            return True
        else:
            print(f"[P2P SEND >>] Message sent unsuccessfully, host is offline")

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
            try:
                p2p_send_socket.close()
            except: # Ignore errors on close
                pass
    return False # Return False if any exception occurred


# --- Channel Functions (create_channel, join_channel) ---
# (Keep these functions as they were in the previous 'final code' version)
def create_channel(reg_socket, channel_name, channel_type):
    """Sends create channel request to registry. Assumes already online."""
    if not MY_NAME or not MY_INFO: print("[ERROR] Cannot create channel: User/Own info missing."); return False
    if not reg_socket: print("[ERROR] Cannot create channel: Not connected (Offline?)."); return False
    try:
        msg = {'type': 'create_channel', 'channel_name': channel_name, 'channel_type': channel_type,
               'owner_name': MY_NAME, 'owner_ip': MY_INFO.get('ip'), 'owner_p2p_port': MY_INFO.get('p2p_port')}
        reg_socket.sendall((json.dumps(msg) + "\n").encode('utf-8')); print(f"[CMD] Channel creation request sent for '{channel_name}'."); return True
    except (socket.error, OSError) as e: print(f"[ERROR] Network error sending create request: {e}"); return False
    except Exception as e: print(f"[ERROR] Failed to send create request: {e}"); return False

def join_channel(channel_name_to_join):
    """Sends P2P join request to channel owner. Assumes already online."""
    global available_channels_info, MY_NAME, MY_INFO, my_channels
    if not channel_name_to_join: print("[JOIN ERROR] Channel name required."); return False
    with my_channels_lock:
        if channel_name_to_join in my_channels: role = "owner" if my_channels[channel_name_to_join].get('owner') == MY_NAME else "member"; print(f"[JOIN INFO] Already a {role} of '{channel_name_to_join}'."); return True
    owner_info = None
    with available_channels_info_lock: owner_info = available_channels_info.get(channel_name_to_join)
    if not owner_info: print(f"[JOIN ERROR] Channel '{channel_name_to_join}' not found in available list (/list_channels)."); return False
    owner_name = owner_info.get('owner_name'); owner_ip = owner_info.get('owner_ip'); owner_port = owner_info.get('owner_p2p_port')
    if not owner_name or not owner_ip or not owner_port: print(f"[JOIN ERROR] Incomplete owner info for '{channel_name_to_join}'."); return False
    if owner_name == MY_NAME: print(f"[JOIN INFO] You are the owner of '{channel_name_to_join}'."); return True
    if not MY_INFO or not MY_NAME: print("[JOIN ERROR] Cannot join: Your own info missing."); return False
    join_req_msg = {'type': 'request_join_channel', 'channel_name': channel_name_to_join, 'requester_name': MY_NAME,
                    'requester_ip': MY_INFO.get('ip'), 'requester_p2p_port': MY_INFO.get('p2p_port')}
    join_req_json = json.dumps(join_req_msg) + "\n"
    print(f"[JOIN] Sending P2P join request for '{channel_name_to_join}' to owner '{owner_name}'...")
    success = send_p2p_message(owner_name, join_req_json)
    if success: print(f"[JOIN] Request sent. Waiting for acceptance from {owner_name}.")
    return success

# --- Send Channel Message (Handles Offline Queuing) ---
def send_channel_msg(channel_name, msg_content, reg_socket):
    """Sends message online, or queues it if offline."""
    global my_channels, MY_NAME, MY_INFO, channel_message_logs, is_offline, offline_message_queue

    # --- Offline: Queue the message ---
    if is_offline:
        timestamp_iso = datetime.datetime.now(timezone.utc).isoformat()
        message_to_queue = {
            'channel_name': channel_name,
            'content': msg_content,
            'timestamp': timestamp_iso # Store original timestamp
        }
        with offline_queue_lock:
            offline_message_queue.append(message_to_queue)

        # Display locally immediately with indication
        try: 
            ts_obj = datetime.datetime.fromisoformat(timestamp_iso); 
            time_str = ts_obj.strftime('%H:%M:%S')
        except: time_str = "time?"
        print(f"[{channel_name} @ {time_str}] {MY_NAME} (Offline Queued): {msg_content}")
        print(f"[MSG OFFLINE QUEUED] Message for '{channel_name}' queued.")
        return True
    # --- End of Offline Handling ---

    # --- Online: Send immediately ---
    with my_channels_lock:
        if channel_name not in my_channels: print(f"[MSG ERROR] Not in channel '{channel_name}'."); return False
        channel_data = my_channels[channel_name]; owner_name = channel_data.get('owner')
        members_dict = channel_data.get('members', {}); is_owner = (owner_name == MY_NAME)
    if not MY_NAME or not MY_INFO: print("[MSG ERROR] Cannot send: User info missing."); return False
    timestamp_iso = datetime.datetime.now(timezone.utc).isoformat() # Timestamp for online send

    if is_owner:
        # Owner Sending (Online) - Broadcast P2P
        message_payload = {'type': 'channel_message', 'channel_name': channel_name,
                           'sender': MY_NAME, 'content': msg_content, 'timestamp': timestamp_iso }
        message_json = json.dumps(message_payload) + "\n"
        with channel_logs_lock: # Log to owner's history
             log_entry = {'timestamp': timestamp_iso, 'sender': MY_NAME, 'content': msg_content}
             channel_message_logs.setdefault(channel_name, []).append(log_entry)
        print(f"[MSG SENDING] Broadcasting to '{channel_name}' members...")
        for member_name in members_dict:
            threading.Thread(target=send_p2p_message, args=(member_name, message_json), daemon=True).start()
        # Owner sees via P2P handler
        # Also send the message to host for storing
        registry_socket.sendall(message_json.encode('utf-8'))
    else:
        # Member Sending (Online) - Forward to Owner P2P or Server
        message_payload = {'type': 'forward_to_owner', 'channel_name': channel_name,
                           'sender': MY_NAME, 'content': msg_content, 'timestamp': timestamp_iso }
        message_json = json.dumps(message_payload) + "\n"
        print(f"[MSG SENDING] Forwarding to owner '{owner_name}' for '{channel_name}'...")
        success = send_p2p_message(owner_name, message_json) # Try P2P first
        if success: # Display own message on successful P2P forward
            try: ts_obj = datetime.datetime.fromisoformat(timestamp_iso); time_str = ts_obj.strftime('%H:%M:%S')
            except: time_str = "time?"
            print(f"[{channel_name} @ {time_str}] {MY_NAME}: {msg_content}")
        else: # P2P Failed, try server log
            print(f"[MSG WARNING] Failed P2P send to owner '{owner_name}'. Sending to server log...")
            if reg_socket:
                try:
                    reg_socket.sendall(message_json.encode('utf-8')); print("[MSG INFO] Message forwarded to server log.")
                    # Display own message on successful server forward
                    try: ts_obj = datetime.datetime.fromisoformat(timestamp_iso); time_str = ts_obj.strftime('%H:%M:%S')
                    except: time_str = "time?"
                    print(f"[{channel_name} @ {time_str}] {MY_NAME}: {msg_content}")
                except Exception as e: print(f"[MSG ERROR] Failed to send message to server log: {e}")
            else: print("[MSG ERROR] Failed P2P to owner and cannot reach server (offline?). Message not sent.")
    return True

# --- Synchronize Between The Host and the Server ---
def synchronize_host_server(reg_socket):
    global channel_message_logs, my_channels, MY_NAME   

    owned_channels_to_check = []
    
    with my_channels_lock:
        owned_channels_to_check = [
            name for name, data in my_channels.items()
            if data.get('owner') == MY_NAME
        ]

    if not owned_channels_to_check:
        print("[OWNER SYNC SERVER] No owned channels found to synchronize with server.")
        return
    for channel_name in owned_channels_to_check:
        content = []
        with channel_logs_lock:
            # Get a copy of the content list to avoid holding lock long
            content = channel_message_logs.get(channel_name, []).copy()

        if not content:
            print(f"[OWNER SYNC SERVER] Owned channel '{channel_name}' has no local messages.")
            continue

        channel_latest_message = content[-1]
        channel_latest_timestamp = channel_latest_message.get('timestamp') 

        if channel_latest_timestamp:
            print(f"[OWNER SYNC SERVER] Latest local message for owned channel '{channel_name}' (ts: {channel_latest_timestamp}):")
            print(f"  -> {channel_latest_message}")

            message = {
                'type': 'synchronization',
                'channel_name': channel_name,
                'content': channel_latest_message,
                'timestamp': channel_latest_timestamp
            }
            message_json = json.dumps(message)
            try:
                # ADD '\n' here!
                reg_socket.sendall((message_json + '\n').encode('utf-8'))
                print(f"[SYNC SEND] Sent sync request for '{channel_name}' with timestamp {channel_latest_timestamp}")
            except Exception as e:
                print(f"[SYNC ERROR] Failed to send sync request for '{channel_name}': {e}")

        else:
            print(f"[OWNER SYNC SERVER WARN] Latest message in owned channel '{channel_name}' lacks a 'timestamp': {channel_latest_message}")

    print("[OWNER SYNC SERVER] Finished checking owned channels.")

# --- Synchronize Offline Messages ---
def synchronize_offline_messages(reg_socket):
    """Processes and sends messages queued while offline."""
    global offline_message_queue, my_channels, channel_message_logs

    messages_to_process = []
    with offline_queue_lock:
        if not offline_message_queue: return # No messages to sync
        messages_to_process = list(offline_message_queue) # Copy queue
        offline_message_queue.clear() # Clear original queue

    print(f"[SYNC] Processing {len(messages_to_process)} queued offline message(s)...")

    for msg_data in messages_to_process:
        channel_name = msg_data.get('channel_name')
        content = msg_data.get('content')
        original_timestamp = msg_data.get('timestamp') # Use the timestamp from when it was typed

        if not channel_name or content is None or not original_timestamp:
            print(f"[SYNC ERROR] Invalid queued data skipped: {msg_data}")
            continue

        # Determine role (owner/member) and destination based on *current* channel state
        owner_name = None
        members_dict = {}
        is_currently_owner = False
        with my_channels_lock:
            channel_info = my_channels.get(channel_name)
            if channel_info:
                owner_name = channel_info.get('owner')
                members_dict = channel_info.get('members', {})
                is_currently_owner = (owner_name == MY_NAME)
            else:
                print(f"[SYNC ERROR] Cannot send queued message for '{channel_name}': Not currently in channel. Discarded.")
                continue # Skip if not in channel anymore

        if is_currently_owner:
            # --- Syncing Owner's Offline Message: Broadcast P2P ---
            print(f"[SYNC] Broadcasting queued owner message for '{channel_name}'...")
            message_payload = {'type': 'channel_message', 'channel_name': channel_name,
                               'sender': MY_NAME, 'content': content, 'timestamp': original_timestamp }
            message_json = json.dumps(message_payload) + "\n"
            # Log locally (again? or rely on original send?) - Let's log here for consistency
            with channel_logs_lock:
                log_entry = {'timestamp': original_timestamp, 'sender': MY_NAME, 'content': content}
                channel_message_logs.setdefault(channel_name, []).append(log_entry)
            # Send P2P to all current members
            for member_name in members_dict:
                if member_name != MY_NAME: # Don't send to self during sync broadcast
                    threading.Thread(target=send_p2p_message, args=(member_name, message_json), daemon=True).start()

        else:
            # --- Syncing Member's Offline Message: Forward P2P or Server ---
            if not owner_name: # Should have owner if in channel, but double check
                print(f"[SYNC ERROR] Cannot send queued message for '{channel_name}': Owner unknown. Discarded.")
                continue

            print(f"[SYNC] Forwarding queued member message for '{channel_name}' to owner '{owner_name}'...")
            message_payload = {'type': 'forward_to_owner', 'channel_name': channel_name,
                               'sender': MY_NAME, 'content': content, 'timestamp': original_timestamp }
            message_json = json.dumps(message_payload) + "\n"
            # Try P2P first
            success = send_p2p_message(owner_name, message_json)
            if not success: # P2P failed, try server
                print(f"[SYNC WARNING] Failed P2P sync to owner '{owner_name}'. Sending to server log...")
                if reg_socket:
                    try: reg_socket.sendall(message_json.encode('utf-8')); print("[SYNC INFO] Queued message forwarded to server log.")
                    except Exception as e: print(f"[SYNC ERROR] Failed to send queued message to server log: {e}. Discarded.")
                else: print("[SYNC ERROR] Failed P2P sync and cannot reach server. Queued message discarded.")
    # End of loop
    print("[SYNC] Finished processing offline queue.")


# --- Initial Login/Guest Function (login_or_guest) ---
# (Keep this function as it was in the previous 'final code' version)
def login_or_guest(host, port):
    """Handles initial login/guest choice and registry connection/ack. Sets global socket."""
    global MY_NAME, MY_ID, is_guest, MY_INFO, running, initial_login_info, registry_socket
    temp_socket = None
    while running:
        try:
            print("\n--- Welcome ---\n  /guest <name>\n  /login <name> <id>\n  (Ctrl+C to exit)")
            cmd = input("> ").strip();
            if not cmd: continue
            cmd_lower = cmd.lower(); login_data = None; potential_login_info = {}

            if cmd_lower.startswith('/guest '):
                parts = cmd.split(' ', 1); name = parts[1].strip() if len(parts)==2 else None
                if name and 0 < len(name) <= 50 and ' ' not in name:
                    MY_NAME=name; MY_ID=None; is_guest=True; update_my_info(MY_NAME, MY_P2P_PORT)
                    login_data = {'type':'guest_login','name':MY_NAME,'ip':MY_INFO.get('ip'),'p2p_port':MY_P2P_PORT}
                    potential_login_info = {'type':'guest','name':MY_NAME}
                else: print("[ERROR] Invalid guest name (1-50 chars, no spaces).")
            elif cmd_lower.startswith('/login '):
                parts = cmd.split(' ', 2); name = parts[1].strip() if len(parts)>1 else None; id_str = parts[2].strip() if len(parts)>2 else None
                if name and 0<len(name)<=50 and ' ' not in name and id_str and id_str.isdigit():
                     MY_NAME=name; MY_ID=id_str; is_guest=False; update_my_info(MY_NAME, MY_P2P_PORT)
                     login_data = {'type':'user_login','name':MY_NAME,'id':MY_ID,'ip':MY_INFO.get('ip'),'p2p_port':MY_P2P_PORT}
                     potential_login_info = {'type':'user','name':MY_NAME,'id':MY_ID}
                else: print("[ERROR] Invalid login format. Usage: /login <name> <id_number>")
            else: print("[ERROR] Unknown command. Use /guest or /login.")

            if login_data:
                try:
                    print(f"[CONNECTING] to Registry {host}:{port}..."); temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM); temp_socket.settimeout(10.0); temp_socket.connect((host, port))
                    print("[CONNECTED] Sending identification..."); login_data['is_invisible'] = is_invisible; temp_socket.sendall((json.dumps(login_data) + "\n").encode('utf-8'))
                    print("[WAITING] for acknowledgement..."); buffer=""; ack_received=False; temp_socket.settimeout(20.0)
                    while not ack_received:
                        try:
                            data=temp_socket.recv(BUFFER_SIZE)
                            if not data: print("[ERROR] Connection closed by server."); ack_received=True; break
                            buffer += data.decode('utf-8')
                            if '\n' in buffer:
                                resp_json, buffer = buffer.split('\n', 1); response = json.loads(resp_json); r_type = response.get('type')
                                if r_type == 'login_ack' or r_type == 'guest_ack':
                                    print(f"[SUCCESS] {response.get('message')}"); server_ip = response.get('client_ip');
                                    if server_ip: update_my_info(MY_NAME, MY_P2P_PORT, ip=server_ip)
                                    ack_received=True; registry_socket = temp_socket; registry_socket.settimeout(None); initial_login_info = potential_login_info; return True
                                elif r_type == 'error': print(f"[FAILED] {response.get('message')}"); ack_received=True; break
                                else: print(f"[WARNING] Unexpected ack response: {r_type}")
                        except socket.timeout: print("[ERROR] Timeout waiting for server acknowledgement."); ack_received=True; break
                        except (socket.error, json.JSONDecodeError, UnicodeDecodeError) as e: print(f"[ERROR] Acknowledgement error: {e}"); ack_received=True; break
                    if registry_socket is None:
                        if temp_socket: temp_socket.close(); temp_socket = None
                        print("[INFO] Registration failed. Please try again."); continue
                except socket.timeout: print(f"[ERROR] Connection to {host}:{port} timed out."); time.sleep(1); continue
                except socket.error as e: print(f"[ERROR] Registry connection error: {e}"); time.sleep(1); continue
                except Exception as e: print(f"[CRITICAL] Login error: {e}"); running=False; return False
        except (EOFError, KeyboardInterrupt): print("\n[EXITING]"); running = False; break
        except Exception as e: print(f"[CRITICAL] Input loop error: {e}"); running = False; break
    if temp_socket: 
        try: temp_socket.close()
        except: pass
    return False

# --- Reconnect and Re-register (reconnect_and_register) ---
# (Keep this function as it was in the previous 'final code' version)
def reconnect_and_register(host, port):
    """Attempts to reconnect to the registry and re-register using stored info."""
    global registry_socket, MY_INFO, initial_login_info, running
    if not initial_login_info: print("[ERROR] Cannot reconnect: Initial login info missing."); return False
    login_data = {}; login_type = initial_login_info.get('type'); name = initial_login_info.get('name'); user_id = initial_login_info.get('id')
    update_my_info(name, MY_P2P_PORT)
    if login_type == 'guest' and name: login_data = {'type': 'guest_login','name': name,'ip': MY_INFO.get('ip'),'p2p_port': MY_P2P_PORT}
    elif login_type == 'user' and name and user_id: login_data = {'type': 'user_login','name': name,'id': user_id,'ip': MY_INFO.get('ip'),'p2p_port': MY_P2P_PORT}
    else: print("[ERROR] Cannot reconnect: Invalid stored login info."); return False
    login_data['is_invisible'] = is_invisible
    temp_socket = None
    try:
        print(f"[RECONNECTING] to Registry {host}:{port}..."); temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM); temp_socket.settimeout(10.0); temp_socket.connect((host, port))
        print("[CONNECTED] Sending identification..."); temp_socket.sendall((json.dumps(login_data) + "\n").encode('utf-8'))
        print("[WAITING] for acknowledgement..."); buffer = ""; ack_received = False; temp_socket.settimeout(20.0)
        while not ack_received:
            try:
                data = temp_socket.recv(BUFFER_SIZE)
                if not data: print("[ERROR] Connection closed by server during reconnect."); break
                buffer += data.decode('utf-8')
                if '\n' in buffer:
                    resp_json, buffer = buffer.split('\n', 1); response = json.loads(resp_json); r_type = response.get('type')
                    if r_type == 'login_ack' or r_type == 'guest_ack':
                        print(f"[RECONNECT SUCCESS] {response.get('message')}"); server_ip = response.get('client_ip');
                        if server_ip: update_my_info(name, MY_P2P_PORT, ip=server_ip)
                        registry_socket = temp_socket; registry_socket.settimeout(None); return True
                    elif r_type == 'error': print(f"[RECONNECT FAILED] {response.get('message')}"); break
                    else: print(f"[WARNING] Unexpected ack response during reconnect: {r_type}")
            except socket.timeout: print("[ERROR] Timeout waiting for reconnect ack."); break
            except (socket.error, json.JSONDecodeError, UnicodeDecodeError) as e: print(f"[ERROR] Reconnect ack error: {e}"); break
    except socket.timeout: print(f"[ERROR] Connection timed out during reconnect.")
    except socket.error as e: print(f"[ERROR] Reconnect connection error: {e}")
    except Exception as e: print(f"[CRITICAL] Reconnect error: {e}")
    if temp_socket: 
        try: temp_socket.close()
        except: pass
    registry_socket = None; return False

# --- Main Client Function ---
def start_p2p_client():
    global running, MY_NAME, MY_INFO, is_offline, initial_login_info, is_guest, MY_ID
    global registry_socket, p2p_listener_thread, registry_listener_thread
    global stop_p2p_listener_event, stop_registry_listener_event

    # --- Initial Login ---
    if not login_or_guest(REGISTRY_HOST, REGISTRY_PORT):
        print("[EXITING] Initial login failed."); running = False; time.sleep(0.5); return

    # --- Start Initial Listeners ---
    is_offline = False
    stop_p2p_listener_event = threading.Event()
    stop_registry_listener_event = threading.Event()
    print(f"[INFO] Starting P2P listener on {LISTEN_HOST}:{MY_P2P_PORT}...")
    p2p_listener_thread = threading.Thread(target=listen_for_peers, args=(LISTEN_HOST, MY_P2P_PORT, stop_p2p_listener_event), daemon=True); p2p_listener_thread.start()
    time.sleep(0.1)
    print("[INFO] Starting Registry listener...")
    if registry_socket:
        registry_listener_thread = threading.Thread(target=listen_to_registry, args=(registry_socket, stop_registry_listener_event), daemon=True); registry_listener_thread.start()
    else: print("[CRITICAL] Registry socket unavailable after login. Forcing offline."); is_offline = True

    # --- Welcome Message ---
    print("-" * 30 + f"\n--- Welcome, {'Guest ' if is_guest else ''}{MY_NAME} " + (f"(ID: {MY_ID})" if not is_guest else "") + " ---")
    print(f"Your Info: {MY_INFO}")
    print("--- Commands: /list /myinfo /msg /create /join /list_channels /my_channels /members /history /quit /invisible /offline /online ---")
    print("-" * 30)

    # --- Main Command Loop ---
    while running:
        try:
            prompt_state = "OFFLINE" if is_offline else MY_NAME; prompt = f"({prompt_state})> "; cmd = input(prompt).strip()
        except (EOFError, KeyboardInterrupt): cmd = "/quit"
        if not running: break
        if not cmd: continue
        cmd_lower = cmd.lower()

        # --- Quit Command ---
        if cmd_lower == '/quit':
            print("[COMMAND] Quit requested.")
            if not is_offline and registry_socket:
                 try: registry_socket.sendall((json.dumps({'type': 'update_offline_status', 'is_offline': True}) + "\n").encode('utf-8'))
                 except Exception: pass
            stop_p2p_listener_event.set(); stop_registry_listener_event.set()
            if registry_socket: 
                try: registry_socket.close()
                except Exception: pass
                registry_socket = None
            running = False; print("[SHUTDOWN] Exiting..."); break

        # --- Offline State Handling ---
        if is_offline:
            if cmd_lower == '/online':
                print("[COMMAND] Attempting to go online...")
                if reconnect_and_register(REGISTRY_HOST, REGISTRY_PORT):
                    print("[STATUS] Reconnected successfully.")
                    is_offline = False
                    try: # Send online status
                         if registry_socket: registry_socket.sendall((json.dumps({'type': 'update_offline_status', 'is_offline': False}) + "\n").encode('utf-8'))
                    except Exception as e: print(f"[ERROR] Failed to send online status update: {e}")
                    # Restart listeners
                    stop_p2p_listener_event = threading.Event(); stop_registry_listener_event = threading.Event()
                    print(f"[INFO] Restarting P2P listener..."); 
                    p2p_listener_thread = threading.Thread(target=listen_for_peers, args=(LISTEN_HOST, MY_P2P_PORT, stop_p2p_listener_event), daemon=True); 
                    p2p_listener_thread.start()
                    time.sleep(0.1)
                    print("[INFO] Restarting Registry listener...")
                    if registry_socket: registry_listener_thread = threading.Thread(target=listen_to_registry, args=(registry_socket, stop_registry_listener_event), daemon=True); registry_listener_thread.start()
                    else: print("[ERROR] Cannot restart Registry listener: Socket missing.")
                                        # Sync offline messages
                    time.sleep(0.1)
                    # This method synchronize between the channel hosting and the server
                    synchronize_host_server(registry_socket)
                    # This method synchronize between the peers
                    synchronize_offline_messages(registry_socket) # Pass current socket
                else: print("[STATUS] Failed to go online.")
            # --- Allowed Offline Commands ---
            elif cmd_lower == '/myinfo': print(f"[YOUR INFO]: {MY_INFO} | Status: {'Guest' if is_guest else f'User (ID: {MY_ID})'} | State: OFFLINE")
            elif cmd_lower == '/my_channels':
                print("[YOUR CHANNELS (Local View)]:")
                with my_channels_lock: channels=sorted(my_channels.items())
                if channels: [print(f"  - {n} ({'Owner' if d.get('owner')==MY_NAME else 'Member'}, Owner: {d.get('owner','?')}, {len(d.get('members',{})) if d.get('owner')==MY_NAME else '?'} members)") for n,d in channels]
                else: print("  (Not in any channels locally)")
            elif cmd_lower.startswith('/history '):
                parts=cmd.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
                if name:
                    is_owner=False; 
                    with my_channels_lock: is_owner = name in my_channels and my_channels[name].get('owner')==MY_NAME
                    if is_owner:
                        print(f"[HISTORY for '{name}'] (Stored locally by owner):"); 
                        log_entries=[]; 
                        with channel_logs_lock: 
                            log_entries=channel_message_logs.get(name,[]).copy()
                        if log_entries:
                            for entry in log_entries:
                                ts=entry.get('timestamp','?'); s=entry.get('sender','?'); c=entry.get('content','')
                                try: t_obj=datetime.datetime.fromisoformat(ts); t_str=t_obj.strftime('%Y-%m-%d %H:%M:%S')
                                except: t_str=ts
                                print(f"  [{t_str}] {s}: {c}")
                        else: print("  (No messages logged locally)")
                    else: print(f"[CMD ERROR] Can only view history for channels you own.")
                else: print("[CMD ERROR] Usage: /history <channel_name>")
            elif cmd_lower.startswith('/members '):
                 parts=cmd.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
                 if name:
                     with my_channels_lock: channel_data=my_channels.get(name)
                     if channel_data:
                         print(f"[MEMBERS of '{name}'] (Local View):"); members=sorted(channel_data.get('members',{}).keys())
                         if members: [print(f"  - {m} {'(Owner)' if m==channel_data.get('owner') else ''} {'(You)' if m==MY_NAME else ''}") for m in members]
                         else: print("  (No members known locally)")
                     else: print(f"[CMD ERROR] Not part of channel '{name}' locally.")
                 else: print("[CMD ERROR] Usage: /members <channel_name>")
            elif cmd_lower.startswith('/msg '): # Queues offline message
                parts=cmd.split(' ',2)
                if len(parts)==3: 
                    channel_name=parts[1].strip()
                    msg_content=parts[2]
                    if channel_name and msg_content: 
                        send_channel_msg(channel_name,msg_content,None) # Socket is None
                    else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")
                else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")
            elif cmd_lower == '/invisible': # Set state for next online
                 is_invisible = not is_invisible; status = "invisible" if is_invisible else "visible"; print(f"[STATUS] Visibility set to {status}. Will apply when next online.")
            elif cmd_lower == '/quit': pass # Handled above
            else: print("[INFO] You are offline. Available: /online, /quit, /myinfo, /my_channels, /members, /history, /msg, /invisible.")
            continue # End of offline command handling

        # --- Online State Commands ---
        elif cmd_lower == '/offline':
            if is_offline: continue
            print("[COMMAND] Going offline...")
            try: # Notify server
                if registry_socket: registry_socket.sendall((json.dumps({'type': 'update_offline_status', 'is_offline': True}) + "\n").encode('utf-8')); print("[STATUS] Offline status notification sent.")
                else: print("[WARNING] Cannot send offline status: Not connected.")
            except Exception as e: print(f"[WARNING] Failed to send offline status notification: {e}")
            stop_p2p_listener_event.set(); stop_registry_listener_event.set() # Signal listeners
            if registry_socket: print("[CLOSING] Registry connection..."); 
            try: registry_socket.close(); 
            except Exception: pass; 
            registry_socket = None
            is_offline = True; print("[STATUS] You are now offline. Network listeners stopped.")

        elif cmd_lower == '/invisible':
            is_invisible = not is_invisible; status = "invisible" if is_invisible else "visible"; print(f"[STATUS] You are now {status}")
            try: # Notify server
                if registry_socket: registry_socket.sendall((json.dumps({'type': 'update_visibility', 'is_invisible': is_invisible}) + "\n").encode('utf-8'))
                else: print("[ERROR] Cannot update visibility: Offline.")
            except Exception as e: print(f"[ERROR] Failed to update visibility status: {e}"); is_invisible = not is_invisible

        elif cmd_lower == '/list':
            print("[CMD] Requesting peer list...")
            try:
                 if registry_socket: registry_socket.sendall((json.dumps({'type':'list','peer_id':f"user_{MY_P2P_PORT}" if not is_guest else f"guest_{MY_P2P_PORT}"})+"\n").encode('utf-8'))
                 else: print("[ERROR] Cannot list peers: Offline.")
            except Exception as e: print(f"[ERROR] Failed to send list request: {e}")

        elif cmd_lower == '/myinfo': print(f"[YOUR INFO]: {MY_INFO} | Status: {'Guest' if is_guest else f'User (ID: {MY_ID})'} | State: ONLINE")

        elif cmd_lower.startswith('/msg '): # Sends online message
            parts=cmd.split(' ',2)
            if len(parts)==3: 
                channel_name=parts[1].strip(); 
                msg_content=parts[2]
                if channel_name and msg_content: send_channel_msg(channel_name,msg_content,registry_socket) # Pass socket
                else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")
            else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")

        elif cmd_lower.startswith('/create '):
            parts=cmd.split(' ',2)
            if len(parts)==3: 
                name=parts[1].strip()
                channel_type=parts[2].strip()
                if name and channel_type: create_channel(registry_socket, name, channel_type)
                else: print("[CMD ERROR] Usage: /create <channel_name> <public/private>")
            else: print("[CMD ERROR] Usage: /create <channel_name> <public/private>")

        elif cmd_lower.startswith('/join '):
             parts=cmd.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
             if name: join_channel(name)
             else: print("[CMD ERROR] Usage: /join <channel_name>")

        elif cmd_lower == '/list_channels':
             print("[CMD] Requesting channel list...")
             try:
                  if registry_socket: registry_socket.sendall((json.dumps({'type': 'request_channel_list'}) + "\n").encode('utf-8'))
                  else: print("[ERROR] Cannot list channels: Offline.")
             except Exception as e: print(f"[ERROR] Sending request: {e}")

        elif cmd_lower == '/my_channels': # Same as offline
            print("[YOUR CHANNELS (Local View)]:")
            with my_channels_lock: channels=sorted(my_channels.items())
            if channels: [print(f"  - {n} ({'Owner' if d.get('owner')==MY_NAME else 'Member'}, Owner: {d.get('owner','?')}, {len(d.get('members',{})) if d.get('owner')==MY_NAME else '?'} members)") for n,d in channels]
            else: print("  (Not in any channels locally)")

        elif cmd_lower.startswith('/members '): # Same as offline
             parts=cmd.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
             if name:
                 with my_channels_lock: channel_data=my_channels.get(name)
                 if channel_data:
                     print(f"[MEMBERS of '{name}'] (Local View):"); members=sorted(channel_data.get('members', {}).keys())
                     if members: [print(f"  - {m} {'(Owner)' if m==channel_data.get('owner') else ''} {'(You)' if m==MY_NAME else ''}") for m in members]
                     else: print("  (No members known locally)")
                 else: print(f"[CMD ERROR] Not part of channel '{name}' locally.")
             else: print("[CMD ERROR] Usage: /members <channel_name>")

        elif cmd_lower.startswith('/history '): # Same as offline
            parts=cmd.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
            if name:
                is_owner=False
                with my_channels_lock: is_owner = name in my_channels and my_channels[name].get('owner')==MY_NAME
                if is_owner:
                    print(f"[HISTORY for '{name}'] (Stored locally by owner):"); 
                    log_entries=[]; 
                    with channel_logs_lock: log_entries=channel_message_logs.get(name,[]).copy()
                    if log_entries:
                        for entry in log_entries:
                            ts=entry.get('timestamp','?'); s=entry.get('sender','?'); c=entry.get('content','')
                            try: t_obj=datetime.datetime.fromisoformat(ts); t_str=t_obj.strftime('%Y-%m-%d %H:%M:%S')
                            except: t_str=ts
                            print(f"  [{t_str}] {s}: {c}")
                    else: print("  (No messages logged locally)")
                else: print(f"[CMD ERROR] Can only view history for channels you own.")
            else: print("[CMD ERROR] Usage: /history <channel_name>")

        else: print("[CMD ERROR] Unknown command.")

    # --- Cleanup (Only reached on /quit) ---
    if not running:
        print("[INFO] Waiting briefly for threads..."); time.sleep(0.5)
        print("[CLOSED] Client shut down complete.")

# --- Entry Point ---
if __name__ == "__main__":
    start_p2p_client()