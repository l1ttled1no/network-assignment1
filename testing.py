# --- p2p_client_gui.py ---
import tkinter as tk
from tkinter import scrolledtext, simpledialog, messagebox, font as tkfont
import socket
import threading
import sys
import time
import json
import random
import datetime
from datetime import timezone
import queue # For thread-safe communication with GUI

# --- Existing P2P Logic (Slightly Modified for GUI) ---
# --- Configuration ---
REGISTRY_HOST = '192.168.1.3'
REGISTRY_PORT = 9999
LISTEN_HOST = '0.0.0.0'
MY_P2P_PORT = random.randint(10000, 60000) # Needs to be set early
BUFFER_SIZE = 4096

# --- Shared State (Keep as is) ---
peer_list_lock = threading.Lock()
known_peers = {}
my_channels_lock = threading.Lock()
my_channels = {}
available_channels_info_lock = threading.Lock()
available_channels_info = {}
channel_logs_lock = threading.Lock()
channel_message_logs = {}
offline_queue_lock = threading.Lock()
offline_message_queue = []

# --- Application Control ---
running = True # Controlled by GUI now
is_offline = True # Start offline until login

# --- Identity and Info ---
MY_NAME = None
MY_ID = None
is_guest = None
MY_INFO = {}
initial_login_info = {}

# --- Network & Thread Management ---
registry_socket = None
p2p_listener_thread = None
registry_listener_thread = None
stop_p2p_listener_event = threading.Event()
stop_registry_listener_event = threading.Event()

# --- Visibility ---
is_invisible = False

# --- GUI Communication Queue ---
gui_queue = queue.Queue()

# --- Helper Functions (Modified Output) ---
def gui_log(message, level="INFO"):
    """Puts messages onto the GUI queue for display."""
    try:
        timestamp = datetime.datetime.now().strftime('%H:%M:%S')
        gui_queue.put(f"[{timestamp} {level}] {message}")
    except Exception as e:
        print(f"Error logging to GUI queue: {e}") # Fallback print

def get_local_ip():
    """Try to get a non-loopback local IP address."""
    try:
        # Try connecting to a public DNS server to find the outgoing IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.1)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        # Fallback: get IP associated with hostname
        try:
            return socket.gethostbyname(socket.gethostname())
        except socket.gaierror:
            # Final fallback: loopback address
            return '127.0.0.1'

def update_my_info(name, p2p_port, ip=None):
    """Updates MY_INFO with determined or server-provided IP."""
    global MY_INFO, MY_P2P_PORT
    # Ensure MY_P2P_PORT is set before calling this
    current_ip = ip if ip else get_local_ip()
    new_info = {'ip': current_ip, 'p2p_port': p2p_port, 'name': name}
    if MY_INFO != new_info:
        MY_INFO = new_info
        source = "(from server)" if ip else "(determined locally)"
        gui_log(f"Updated own info {source}: {MY_INFO}")
    # Update the GUI status bar if the GUI is running
    try:
        gui_queue.put(("UPDATE_STATUS", MY_NAME, is_offline, is_invisible))
    except NameError: # gui_queue might not exist early
        pass

def get_peer_name_by_address(ip, port):
    """Looks up peer name from known_peers or returns IP:Port."""
    if MY_INFO.get('ip') == ip and MY_INFO.get('p2p_port') == port:
        return f"{MY_NAME} (Self)"
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('ip') == ip and info.get('p2p_port') == port:
                return info.get('name', f"{ip}:{port}")
    with my_channels_lock:
        for channel in my_channels.values():
            for member_name, member_info in channel.get('members', {}).items():
                if member_info.get('ip') == ip and member_info.get('p2p_port') == port:
                    return member_name
    return f"{ip}:{port}"

def get_peer_info_by_name(name):
    """Looks up peer IP/Port from known_peers or local channels."""
    if name == MY_NAME:
        return MY_INFO
    with peer_list_lock:
        for peer_id, info in known_peers.items():
            if info.get('name') == name:
                return info
    with my_channels_lock:
        for channel in my_channels.values():
            member_info = channel.get('members', {}).get(name)
            if member_info:
                return member_info
    return None

# --- P2P Listener (Modified Output) ---
def listen_for_peers(host, port, stop_event):
    """Starts P2P listener thread and stops when event is set."""
    p2p_server_socket = None # Initialize
    try:
        p2p_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        p2p_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        p2p_server_socket.bind((host, port))
        p2p_server_socket.listen(5)
        gui_log(f"P2P LISTENING on {host}:{port}")
        p2p_server_socket.settimeout(1.0) # Use timeout for periodic checks
        while running and not stop_event.is_set():
            try:
                peer_socket, peer_address = p2p_server_socket.accept()
                # Start a new thread for each connection
                peer_thread = threading.Thread(target=handle_peer_connection, args=(peer_socket, peer_address), daemon=True)
                peer_thread.start()
            except socket.timeout:
                continue # Just checking the stop_event
            except OSError as e:
                if not stop_event.is_set() and running:
                    gui_log(f"P2P LISTENER ERROR] Accept error: {e}", level="ERROR")
                break # Exit loop on critical error
            except Exception as e:
                if running and not stop_event.is_set():
                    gui_log(f"P2P LISTENER CRITICAL ERROR] Unexpected error: {e}", level="CRITICAL")
                break
    except OSError as e:
        # Specific error for bind failure
        if running and not stop_event.is_set():
             gui_log(f"P2P LISTENER BIND ERROR] Could not bind to {host}:{port} - {e}. Try a different port or check permissions.", level="ERROR")
    except Exception as e:
        if running and not stop_event.is_set():
             gui_log(f"P2P LISTENER CRITICAL] Other error: {e}", level="CRITICAL")
    finally:
        gui_log("P2P LISTENING] Shutting down P2P listener.")
        if p2p_server_socket:
            try:
                p2p_server_socket.close()
            except Exception as e:
                gui_log(f"P2P LISTENER WARN] Error closing listener socket: {e}", level="WARN")

# --- P2P Connection Handler (Modified Output) ---
def handle_peer_connection(peer_socket, peer_address):
    """Handles incoming P2P connection and processes messages."""
    # Identify peer once at the beginning
    actual_peer_name = get_peer_name_by_address(peer_address[0], peer_address[1])
    gui_log(f"P2P Connection established with {actual_peer_name} ({peer_address[0]}:{peer_address[1]})")

    buffer = ""
    try:
        while running: # Check global running flag
            try:
                # Receive data from the peer
                data = peer_socket.recv(BUFFER_SIZE)
                if not data:
                    # Connection closed by peer
                    gui_log(f"P2P Connection closed by {actual_peer_name}")
                    break
            except ConnectionResetError:
                gui_log(f"P2P Connection reset by {actual_peer_name}", level="WARN")
                break
            except socket.timeout:
                # Can happen if socket has a timeout, though typically we don't set one here
                gui_log(f"P2P Socket timeout with {actual_peer_name}", level="WARN")
                continue # Or break, depending on desired behavior
            except socket.error as e:
                # Other socket errors
                 gui_log(f"P2P Socket error with {actual_peer_name}: {e}", level="ERROR")
                 break

            # Decode received data
            try:
                buffer += data.decode('utf-8')
            except UnicodeDecodeError:
                 gui_log(f"P2P Received non-UTF8 data from {actual_peer_name}. Discarding buffer.", level="ERROR")
                 buffer = "" # Clear buffer on decoding error
                 continue

            # Process complete messages (newline delimited)
            while '\n' in buffer and running:
                message_json, buffer = buffer.split('\n', 1)
                if not message_json.strip(): # Skip empty lines
                    continue

                try:
                    message = json.loads(message_json)
                    msg_type = message.get('type')

                    # --- Join Request ---
                    if msg_type == 'request_join_channel':
                        req_channel = message.get('channel_name')
                        req_name = message.get('requester_name')
                        req_ip = message.get('requester_ip')
                        req_port = message.get('requester_p2p_port')

                        # Update actual_peer_name if we got it from the request
                        if req_name:
                            actual_peer_name = req_name

                        gui_log(f"P2P RECV] Join request for '{req_channel}' from '{req_name}'")

                        # Validate request data
                        if not (req_channel and req_name and req_ip and req_port):
                             gui_log(f"P2P WARNING] Invalid join request from {actual_peer_name}: Missing fields", level="WARN")
                             continue

                        # Check ownership and process
                        with my_channels_lock:
                            if req_channel in my_channels and my_channels[req_channel].get('owner') == MY_NAME:
                                channel_data = my_channels[req_channel]
                                members_dict = channel_data.setdefault('members', {}) # Get or create members dict

                                requester_info = {'name': req_name, 'ip': req_ip, 'p2p_port': req_port}

                                # Add or update member
                                if req_name not in members_dict:
                                     gui_log(f"CHANNEL] Adding '{req_name}' to '{req_channel}'.")
                                else:
                                     gui_log(f"CHANNEL INFO] '{req_name}' re-requested join to '{req_channel}'. Updating info.")
                                members_dict[req_name] = requester_info

                                # Send acceptance response back P2P in a new thread
                                accept_response = {
                                    'type': 'join_channel_accepted',
                                    'channel_name': req_channel,
                                    'owner_name': MY_NAME,
                                    'members': members_dict # Send the current member list
                                }
                                threading.Thread(target=send_p2p_message,
                                                 args=(req_name, json.dumps(accept_response) + '\n'),
                                                 daemon=True).start()
                                gui_log(f"P2P >>] Sent join acceptance for '{req_channel}' to '{req_name}'.")
                                # Notify GUI to update channel/member lists
                                gui_queue.put(("UPDATE_MY_CHANNELS", my_channels.copy()))


                            elif req_channel in my_channels:
                                 gui_log(f"P2P WARNING] Join request for '{req_channel}' from '{req_name}', but not owner ({my_channels[req_channel].get('owner', '?')}). Ignoring.", level="WARN")
                                # Optionally send a rejection message?
                                # reject_response = {'type': 'join_error', 'channel_name': req_channel, 'reason': 'Not channel owner'}
                                # threading.Thread(target=send_p2p_message, args=(req_name, json.dumps(reject_response) + '\n'), daemon=True).start()

                            else:
                                 gui_log(f"P2P WARNING] Join request from '{req_name}' for unknown local channel '{req_channel}'. Ignoring.", level="WARN")
                                # reject_response = {'type': 'join_error', 'channel_name': req_channel, 'reason': 'Channel not found by this peer'}
                                # threading.Thread(target=send_p2p_message, args=(req_name, json.dumps(reject_response) + '\n'), daemon=True).start()

                    # --- Join Acceptance ---
                    elif msg_type == 'join_channel_accepted':
                        accepted_channel = message.get('channel_name')
                        accepted_owner = message.get('owner_name')
                        accepted_members = message.get('members', {}) # Owner sends the member list

                        gui_log(f"P2P RECV] Successfully joined channel '{accepted_channel}' (Owner: {accepted_owner})!")
                        gui_log(f"CHANNEL] Members in '{accepted_channel}': {list(accepted_members.keys())}")

                        # Update local channel state
                        with my_channels_lock:
                            my_channels[accepted_channel] = {
                                'owner': accepted_owner,
                                'members': accepted_members
                            }
                        # Notify GUI to update channel/member lists
                        gui_queue.put(("UPDATE_MY_CHANNELS", my_channels.copy()))


                    # --- Join Error ---
                    elif msg_type == 'join_error':
                        failed_channel = message.get('channel_name', 'N/A')
                        reason = message.get('reason', 'Unknown reason')
                        gui_log(f"P2P RECV] Join Failed for '{failed_channel}': {reason}", level="ERROR")
                        # Maybe remove from available channels if appropriate?

                    # --- Channel Message ---
                    elif msg_type == 'channel_message':
                        channel = message.get('channel_name')
                        sender = message.get('sender')
                        content = message.get('content')
                        timestamp_str = message.get('timestamp', 'N/A') # ISO format string expected

                        if channel and sender and content:
                            # Check if we are actually a member of this channel locally
                            is_member = False
                            with my_channels_lock:
                                is_member = channel in my_channels

                            if is_member:
                                # Format timestamp for display
                                try:
                                    ts_obj = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')) # Handle Z for UTC
                                    # Convert to local time for display
                                    local_ts = ts_obj.astimezone()
                                    time_str = local_ts.strftime('%H:%M:%S')
                                except (ValueError, TypeError):
                                    time_str = "time?" # Fallback

                                # Determine display name (add 'You' if it's our own message coming back)
                                display_sender = f"{sender} (You)" if sender == MY_NAME else sender

                                # Format message for GUI display
                                formatted_msg = f"[{channel} @ {time_str}] {display_sender}: {content}"
                                gui_queue.put(("CHANNEL_MSG", channel, formatted_msg))
                                gui_queue.put(("NEW_MESSAGE_NOTICE", channel))

                            else:
                                # Received a message for a channel we're not in (shouldn't happen if logic is correct)
                                gui_log(f"P2P WARNING] Received message for channel '{channel}' but not a member. Ignoring.", level="WARN")
                        else:
                            gui_log(f"P2P WARNING] Received incomplete channel message from {actual_peer_name}", level="WARN")


                    # --- Forward Request (Received by Owner) ---
                    elif msg_type == 'forward_to_owner':
                        channel = message.get('channel_name')
                        original_sender = message.get('sender') # The member who sent it
                        msg_content = message.get('content')
                        # Use provided timestamp or default to now (UTC)
                        timestamp = message.get('timestamp', datetime.datetime.now(timezone.utc).isoformat())

                        if not (channel and original_sender and msg_content is not None):
                            gui_log(f"P2P WARNING] Invalid 'forward_to_owner' message from {actual_peer_name}: Missing fields", level="WARN")
                            continue

                        is_owner_of_channel = False
                        members_to_forward = {}
                        with my_channels_lock:
                            if channel in my_channels and my_channels[channel].get('owner') == MY_NAME:
                                is_owner_of_channel = True
                                # Get a copy of members to avoid holding lock while sending
                                members_to_forward = my_channels[channel].get('members', {}).copy()

                        if is_owner_of_channel:
                            gui_log(f"P2P RECV] Received message from '{original_sender}' for owned channel '{channel}'. Forwarding...")

                            # 1. Log the message locally (Owner's log)
                            with channel_logs_lock:
                                log_entry = {
                                    'timestamp': timestamp,
                                    'sender': original_sender,
                                    'content': msg_content
                                }
                                channel_message_logs.setdefault(channel, []).append(log_entry)
                                # Maybe notify GUI about history update? (Optional)

                            # 2. Display the message for the owner immediately
                            try:
                                ts_obj = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                local_ts = ts_obj.astimezone()
                                time_str = local_ts.strftime('%H:%M:%S')
                            except (ValueError, TypeError):
                                time_str = "time?"
                            formatted_msg = f"[{channel} @ {time_str}] {original_sender}: {msg_content}"
                            gui_queue.put(("CHANNEL_MSG", channel, formatted_msg))
                            gui_queue.put(("NEW_MESSAGE_NOTICE",channel))

                            # 3. Prepare the message for broadcasting to other members
                            forward_payload = {
                                'type': 'channel_message', # Broadcast as a standard channel message
                                'channel_name': channel,
                                'sender': original_sender, # Keep the original sender
                                'content': msg_content,
                                'timestamp': timestamp
                            }
                            forward_json = json.dumps(forward_payload) + "\n"

                            # 4. Broadcast P2P to all members (excluding owner and original sender)
                            send_count = 0
                            for member_name, member_info in members_to_forward.items():
                                if member_name != MY_NAME and member_name != original_sender:
                                    threading.Thread(target=send_p2p_message,
                                                     args=(member_name, forward_json),
                                                     daemon=True).start()
                                    send_count += 1
                            gui_log(f"P2P >>] Forwarded message from '{original_sender}' to {send_count} members of '{channel}'.")

                            # 5. Send to registry for storage/potential offline delivery
                            if registry_socket and not is_offline:
                                try:
                                    registry_socket.sendall(forward_json.encode('utf-8')) # Note: using forward_json here
                                    gui_log(f"P2P >>] Sent forwarded message for '{channel}' to registry.")
                                except (socket.error, OSError) as e:
                                    gui_log(f"P2P ERROR] Failed to send forwarded message to registry: {e}", level="ERROR")
                            else:
                                 gui_log(f"P2P INFO] Not sending forwarded message to registry (offline or no socket).")


                        else:
                             # Received forward request but not the owner (or channel unknown)
                             gui_log(f"P2P WARNING] Received 'forward_to_owner' from {actual_peer_name} for '{channel}', but not the owner or channel unknown. Ignoring.", level="WARN")


                    else:
                         # Handle unknown message types
                         gui_log(f"P2P UNHANDLED from {actual_peer_name}]: Type: {msg_type}, Content: {message_json}", level="WARN")

                except json.JSONDecodeError:
                    gui_log(f"P2P ERROR] Invalid JSON received from {actual_peer_name}: {message_json}", level="ERROR")
                    # Potentially break connection if persistent invalid JSON?
                except Exception as e:
                    # Catch other processing errors
                    gui_log(f"P2P ERROR] Error processing message from {actual_peer_name}: {e}", level="ERROR")
                    import traceback
                    traceback.print_exc() # Print traceback for debugging

    except Exception as e:
        # Catch errors in the outer loop (e.g., socket closing unexpectedly)
        if running: # Avoid logging errors during normal shutdown
            gui_log(f"P2P HANDLER CRITICAL] Error in connection handler for {actual_peer_name}: {e}", level="CRITICAL")
    finally:
        # Cleanup: Close the peer socket
        gui_log(f"P2P Closing connection with {actual_peer_name}.")
        try:
            # Attempt graceful shutdown
            peer_socket.shutdown(socket.SHUT_RDWR)
        except (socket.error, OSError):
            pass # Ignore errors if already closed or shutdown fails
        try:
            peer_socket.close()
        except (socket.error, OSError):
            pass # Ignore errors on close


# --- Registry Listener (Modified Output) ---
def listen_to_registry(reg_socket, stop_event):
    """Listens for updates from the registry and stops when event is set."""
    global running, known_peers, MY_INFO, my_channels, available_channels_info
    if not reg_socket:
        gui_log("REGISTRY LISTENER] Error: No registry socket provided.", level="ERROR")
        return

    buffer = ""
    reg_socket.settimeout(1.0) # Non-blocking check

    gui_log("REGISTRY LISTENER] Starting...")
    while running and not stop_event.is_set():
        try:
            data = reg_socket.recv(BUFFER_SIZE)
            if not data:
                if running and not stop_event.is_set():
                    gui_log("CONNECTION LOST] Registry server closed connection.", level="WARN")
                    # Trigger offline mode in GUI maybe?
                    gui_queue.put(("FORCE_OFFLINE", "Registry connection lost"))
                break # Exit loop

            try:
                buffer += data.decode('utf-8')
            except UnicodeDecodeError:
                gui_log("REGISTRY ERROR] Invalid UTF-8 data received.", level="ERROR")
                buffer = "" # Clear buffer on error
                continue

            # Process complete messages (newline delimited)
            while '\n' in buffer:
                if not running or stop_event.is_set(): break # Check again before processing
                message_json, buffer = buffer.split('\n', 1)
                if not message_json.strip(): continue # Skip empty lines

                try:
                    message = json.loads(message_json)
                    msg_type = message.get('type')

                    # --- Peer List ---
                    if msg_type == 'peer_list':
                        server_peers = message.get('peers', {})
                        gui_log("REGISTRY RECV] Received updated peer list.")
                        with peer_list_lock:
                            known_peers = server_peers
                        # Send peer list to GUI for display
                        gui_queue.put(("UPDATE_PEERS", known_peers.copy())) # Send a copy

                    # --- Peer Joined ---
                    elif msg_type == 'peer_joined':
                        peer_id = message.get('id')
                        info = message.get('peer_info')
                        name = info.get('name', 'Unknown') if info else 'Unknown'
                        is_invisible_peer = info.get('is_invisible', False) if info else False

                        # Add peer if valid, not self, and visible
                        if peer_id and info and name != MY_NAME and not is_invisible_peer:
                            with peer_list_lock:
                                known_peers[peer_id] = info
                            gui_log(f"REGISTRY] Peer joined: {name}")
                            # Update GUI peer list
                            gui_queue.put(("UPDATE_PEERS", known_peers.copy()))


                    # --- Peer Left ---
                    elif msg_type == 'peer_left':
                        peer_id = message.get('id')
                        if peer_id:
                            removed_name = "Unknown"
                            with peer_list_lock:
                                if peer_id in known_peers:
                                    removed_name = known_peers.pop(peer_id).get('name', 'Unknown')
                            if removed_name != "Unknown":
                                gui_log(f"REGISTRY] Peer left: {removed_name}")
                                # Update GUI peer list
                                gui_queue.put(("UPDATE_PEERS", known_peers.copy()))


                    # --- Channel Created Confirmation ---
                    elif msg_type == 'channel_created':
                        c_name = message.get('channel_name')
                        c_owner = message.get('owner_name')
                        gui_log(f"REGISTRY] Channel '{c_name}' creation confirmed (Owner: {c_owner}).")
                        # If it's our channel, add it to our local 'my_channels'
                        if c_owner == MY_NAME:
                            with my_channels_lock:
                                if c_name not in my_channels:
                                    # Ensure MY_INFO is populated before creating channel entry
                                    if not MY_INFO:
                                        update_my_info(MY_NAME, MY_P2P_PORT) # Update if needed
                                    # Add owner as the first member
                                    my_channels[c_name] = {
                                        'owner': MY_NAME,
                                        'members': {MY_NAME: MY_INFO.copy()}
                                    }
                                    gui_log(f"CHANNEL] Added owned channel '{c_name}' to local list.")
                                    # Notify GUI of the change
                                    gui_queue.put(("UPDATE_MY_CHANNELS", my_channels.copy()))
                        # Also update available channels list
                        owner_ip = message.get('owner_ip') # Server should provide this
                        owner_port = message.get('owner_p2p_port')
                        if c_name and c_owner and owner_ip and owner_port:
                            with available_channels_info_lock:
                                available_channels_info[c_name] = {
                                    'owner_name': c_owner,
                                    'owner_ip': owner_ip,
                                    'owner_p2p_port': owner_port,
                                    # Add channel type if provided by server?
                                    'channel_type': message.get('channel_type', 'unknown')
                                }
                            gui_queue.put(("UPDATE_AVAILABLE_CHANNELS", available_channels_info.copy()))

                    # --- Channel Error ---
                    elif msg_type == 'channel_error':
                        error_msg = message.get('message', 'Unknown channel error')
                        c_name = message.get('channel_name', 'N/A')
                        gui_log(f"REGISTRY ERROR] Channel '{c_name}': {error_msg}", level="ERROR")


                    # --- New Channel Available ---
                    elif msg_type == 'new_channel_available':
                        c_name = message.get('channel_name')
                        c_owner = message.get('owner_name')
                        c_ip = message.get('owner_ip')
                        c_port = message.get('owner_p2p_port')
                        c_type = message.get('channel_type', 'unknown') # Get type if available

                        gui_log(f"REGISTRY] New channel available: '{c_name}' (Owner: {c_owner}, Type: {c_type})")
                        if c_name and c_owner and c_ip and c_port:
                            with available_channels_info_lock:
                                available_channels_info[c_name] = {
                                    'owner_name': c_owner,
                                    'owner_ip': c_ip,
                                    'owner_p2p_port': c_port,
                                    'channel_type': c_type
                                }
                            # Update GUI available channels list
                            gui_queue.put(("UPDATE_AVAILABLE_CHANNELS", available_channels_info.copy()))

                    # --- Full Channel List ---
                    elif msg_type == 'channel_list':
                        gui_log("REGISTRY RECV] Received list of all available channels.")
                        channels_data = message.get('channels', {}) # Expect dict {name: info}
                        with available_channels_info_lock:
                            available_channels_info = channels_data # Replace local list
                        # Update GUI available channels list
                        gui_queue.put(("UPDATE_AVAILABLE_CHANNELS", available_channels_info.copy()))


                    # --- Generic Server Error ---
                    elif msg_type == 'error':
                        error_msg = message.get('message', 'Unknown server error')
                        gui_log(f"SERVER ERROR] {error_msg}", level="ERROR")


                    # --- Server Shutdown Notification ---
                    elif msg_type == 'server_shutdown':
                        shutdown_msg = message.get('message', 'Server is shutting down')
                        gui_log(f"REGISTRY] {shutdown_msg}", level="WARN")
                        # Force client offline
                        gui_queue.put(("FORCE_OFFLINE", "Server initiated shutdown"))
                        # running = False # Should we stop the client entirely? Depends.
                        # stop_event.set() # Stop this listener thread
                        break # Exit listener loop


                    # --- Sync Reply (from Server) ---
# --- Inside listen_to_registry function ---

                # ... (previous message type handlers) ...

                    elif msg_type == 'sync_reply':
                        channel_name = message.get('channel_name')
                        received_messages = message.get('messages') # List of message dicts
                        return_status = message.get('status') # 'sync_point_found', 'sync_point_not_found', etc.

                        # Basic validation
                        if not channel_name or received_messages is None or return_status is None:
                            # Use print for errors in background threads, or a dedicated error queue
                            print(f"[SYNC REPLY ERROR] Received incomplete sync_reply: {message}")
                            # gui_log is problematic here as it puts back on queue
                            # self.log_message(f"SYNC REPLY ERROR] Received incomplete sync_reply: {message}", level="ERROR") # Avoid calling GUI directly
                            continue

                        # Log the sync reply status (minimal logging)
                        print(f"[SYNC REPLY] Received reply for '{channel_name}', status: '{return_status}', messages: {len(received_messages)}")

                        # Handle different statuses (logging only, main logic below)
                        if return_status == 'sync_point_not_found':
                            if not received_messages:
                                print(f"[SYNCHRONIZATION] Server didn't find sync point for '{channel_name}', no history provided.")
                            else:
                                print(f"[SYNCHRONIZATION] Server didn't find sync point for '{channel_name}', receiving full log ({len(received_messages)} messages).")
                        elif return_status == 'sync_point_found':
                            if not received_messages:
                                print(f"[SYNCHRONIZATION] Sync point found for '{channel_name}', no newer messages.")
                                continue # Nothing more to do for this message
                            else:
                                print(f"[SYNCHRONIZATION] Sync point found for '{channel_name}', receiving {len(received_messages)} new/subsequent messages.")
                        else:
                            print(f"[SYNC REPLY WARNING] Received unknown status '{return_status}' for channel '{channel_name}'")


                        # --- Process received messages ---
                        if received_messages:
                            messages_added_to_log_count = 0

                            # 1. Update Owner's Persistent Log (Optional but good for consistency)
                            # Check if the current user is the owner of this channel
                            is_owner_of_channel = False
                            with my_channels_lock: # Check ownership safely
                                channel_info = my_channels.get(channel_name)
                                if channel_info and channel_info.get('owner') == MY_NAME:
                                    is_owner_of_channel = True

                            if is_owner_of_channel:
                                with channel_logs_lock:
                                    log_for_channel = channel_message_logs.setdefault(channel_name, [])
                                    # Add only messages not already present (basic timestamp check)
                                    existing_timestamps = {msg['timestamp'] for msg in log_for_channel}
                                    new_messages_to_add = []
                                    for msg in received_messages:
                                        ts = msg.get('timestamp')
                                        if ts and ts not in existing_timestamps:
                                            new_messages_to_add.append(msg)
                                            existing_timestamps.add(ts) # Avoid adding duplicates from same sync

                                    if new_messages_to_add:
                                        log_for_channel.extend(new_messages_to_add)
                                        messages_added_to_log_count = len(new_messages_to_add)
                                        # Sort log by timestamp after adding
                                        try:
                                            log_for_channel.sort(key=lambda x: datetime.datetime.fromisoformat(x['timestamp'].replace('Z', '+00:00')))
                                        except (KeyError, ValueError, TypeError) as e:
                                            print(f"[SYNC WARN] Could not sort persistent log for '{channel_name}' after sync: {e}")

                                if messages_added_to_log_count > 0:
                                    print(f"[SYNC INFO] Added {messages_added_to_log_count} unique messages to persistent log for '{channel_name}'.")


                            # 2. Format and Queue ALL received messages for GUI Display / Buffering
                            # This happens regardless of ownership
                            print(f"[SYNC INFO] Queuing {len(received_messages)} received messages for GUI display/buffer for '{channel_name}'.")
                            for msg_data in received_messages:
                                try:
                                    sender = msg_data.get('sender', '?')
                                    content = msg_data.get('content', '')
                                    timestamp_str = msg_data.get('timestamp')

                                    if not timestamp_str: # Skip if essential info missing
                                        print(f"[SYNC WARN] Skipping synced message in '{channel_name}' due to missing timestamp: {msg_data}")
                                        continue

                                    # Format timestamp for display
                                    try:
                                        # Handle potential 'Z' for UTC timezone
                                        ts_obj = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                        # Convert to local time for display
                                        local_ts = ts_obj.astimezone()
                                        time_str = local_ts.strftime('%H:%M:%S')
                                    except (ValueError, TypeError):
                                        time_str = "time?" # Fallback

                                    # Format message string for display
                                    formatted_msg = f"[{channel_name} @ {time_str}] {sender}: {content}"

                                    # Put the standard tuple onto the GUI queue
                                    # _process_gui_queue will pick this up and call _display_channel_message
                                    gui_queue.put(("CHANNEL_MSG", channel_name, formatted_msg))

                                except Exception as e:
                                    # Log errors during formatting/queuing for individual messages
                                    print(f"[SYNC ERROR] Error processing synced message for GUI queue: {e} - Data: {msg_data}")
                                    import traceback
                                    traceback.print_exc() # Helps debugging background errors

                            # --- IMPORTANT ---
                            # If the channel being synced is the *currently active* one in the GUI,
                            # the messages queued above will be displayed immediately by _display_channel_message.
                            # If it's *not* the active channel, they will just be added to the buffer
                            # in self.channel_display_buffers and shown when the user selects that channel later.

                    # ... (rest of the message handlers in listen_to_registry) ...


                    # --- Notify New Message (Real-time push from Server) ---
                    elif msg_type == 'notify_new_msg':
                        channel_name = message.get('channel_name')
                        sender = message.get('sender')
                        content = message.get('content')
                        timestamp_str = message.get('timestamp') # ISO format string

                        # Basic Validation
                        if not channel_name or sender is None or content is None or timestamp_str is None:
                            gui_log(f"NOTIFY ERROR] Incomplete 'notify_new_msg' received: {message}", level="ERROR")
                            continue

                        # Check if we are a member of this channel
                        is_member = False
                        with my_channels_lock:
                            is_member = channel_name in my_channels

                        if is_member:
                            # Format timestamp
                            try:
                                ts_obj = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                local_ts = ts_obj.astimezone()
                                time_str = local_ts.strftime('%H:%M:%S')
                            except (ValueError, TypeError):
                                time_str = "time?"

                            # Format message for GUI
                            formatted_msg = f"[{channel_name} @ {time_str}] {sender}: {content}"
                            gui_queue.put(("CHANNEL_MSG", channel_name, formatted_msg))

                            # If owner, also add to local log? Server might be the source of truth,
                            # but adding here ensures consistency if server message is missed later.
                            # Check if owner before logging
                            is_owner_of_channel = False
                            with my_channels_lock:
                                if channel_name in my_channels and my_channels[channel_name].get('owner') == MY_NAME:
                                    is_owner_of_channel = True

                            if is_owner_of_channel:
                                with channel_logs_lock:
                                    log_entry = {
                                        'timestamp': timestamp_str, # Store original ISO string
                                        'sender': sender,
                                        'content': content
                                    }
                                    # Avoid duplicates if already added via P2P forward? Check needed.
                                    log_list = channel_message_logs.setdefault(channel_name, [])
                                    # Simple check: don't add if last message is identical
                                    if not log_list or log_list[-1]['timestamp'] != timestamp_str or log_list[-1]['sender'] != sender or log_list[-1]['content'] != content:
                                        log_list.append(log_entry)
                                        # Optional: Sort log after adding
                                        try:
                                            log_list.sort(key=lambda x: datetime.datetime.fromisoformat(x['timestamp'].replace('Z', '+00:00')))
                                        except (KeyError, ValueError, TypeError):
                                            pass # Ignore sorting errors silently maybe
                                    # else: gui_log(f"NOTIFY INFO] Duplicate message for {channel_name} from registry ignored.")


                        else:
                            # Not a member, ignore silently as intended
                            pass

                    # --- Unknown Message Type ---
                    else:
                        gui_log(f"REGISTRY UNHANDLED] Type: {msg_type}, Content: {message_json}", level="WARN")

                except json.JSONDecodeError:
                     gui_log(f"REGISTRY ERROR] Invalid JSON received: {message_json}", level="ERROR")
                except Exception as e:
                    gui_log(f"REGISTRY ERROR] Error processing message: {e}\nMessage: {message_json}", level="ERROR")
                    import traceback
                    traceback.print_exc() # For debugging

        except socket.timeout:
            continue # Just means no data received, check running/stop_event again
        except (ConnectionResetError, ConnectionAbortedError, OSError) as e:
            if running and not stop_event.is_set():
                gui_log(f"REGISTRY LISTENER ERROR] Connection error: {e}", level="ERROR")
                gui_queue.put(("FORCE_OFFLINE", "Registry connection error"))
            break # Exit loop on major connection error
        except Exception as e:
            if running and not stop_event.is_set():
                 gui_log(f"REGISTRY LISTENER CRITICAL] Unexpected error: {e}", level="CRITICAL")
            break # Exit loop

        # finally:
            # gui_log("REGISTRY LISTENER] Listener thread finished.")
            # # Optionally try to close the socket if it still exists
            # if reg_socket:
            #     try: reg_socket.close()
            #     except: pass


# --- P2P Message Sending (Modified Output) ---
def send_p2p_message(target_name, message_json_with_newline):
    """Sends a pre-formatted JSON message string directly to a peer's P2P listener."""

    # 1. Check if THIS client is offline
    if is_offline:
        gui_log(f"P2P SEND ERROR] Cannot send to '{target_name}': You are offline.", level="ERROR")
        return False

    # 2. Find target info (allow searching both known_peers and my_channels)
    target_info = get_peer_info_by_name(target_name)
    if not target_info:
        gui_log(f"P2P SEND ERROR] Peer '{target_name}' not found.", level="ERROR")
        return False

    # 3. Check target's offline status *if available in known_peers*
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
    
    # 4. Get IP and Port from the found target_info
    ip, port = target_info.get('ip'), target_info.get('p2p_port')
    if not ip or not port:
        gui_log(f"P2P SEND ERROR] Peer '{target_name}' has incomplete info (IP/Port missing).", level="ERROR")
        return False

    # 5. Attempt P2P connection and send
    p2p_send_socket = None
    try:
        if found_in_registry:
            gui_log(f"P2P SEND >>] Attempting P2P to '{target_name}' at {ip}:{port}")

            p2p_send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            p2p_send_socket.settimeout(5.0) # Connection timeout
            p2p_send_socket.connect((ip, port))
            p2p_send_socket.sendall(message_json_with_newline.encode('utf-8'))
            # gui_log(f"[P2P SEND OK] Message sent successfully to '{target_name}'.") # Maybe too verbose
            return True
        else:
            print(f"[P2P SEND >>] Message sent unsuccessfully, host is offline")
            return False

    except socket.timeout:
        gui_log(f"P2P SEND ERROR] Connection to '{target_name}' ({ip}:{port}) timed out.", level="ERROR")
    except ConnectionRefusedError:
        gui_log(f"P2P SEND ERROR] Connection to '{target_name}' ({ip}:{port}) refused. Peer might be offline or listener down.", level="ERROR")
        # Maybe update local status if possible? Tricky.
    except OSError as e:
        # Network unreachable, etc.
        gui_log(f"P2P SEND ERROR] Network error sending to '{target_name}' ({ip}:{port}): {e}", level="ERROR")
    except Exception as e:
        gui_log(f"P2P SEND ERROR] Unexpected error sending to '{target_name}': {e}", level="ERROR")
        import traceback
        traceback.print_exc()
    finally:
        # Ensure the socket is closed
        if p2p_send_socket:
            try:
                p2p_send_socket.close()
            except:
                pass # Ignore errors on close

    return False # Return False if any exception occurred or connection failed


# --- Channel Functions (Modified Output) ---
def create_channel(reg_socket, channel_name, channel_type):
    """Sends create channel request to registry. Assumes already online."""
    if not MY_NAME or not MY_INFO:
        gui_log("ERROR] Cannot create channel: User/Own info missing.", level="ERROR")
        return False
    if not reg_socket or is_offline: # Check offline status too
        gui_log("ERROR] Cannot create channel: Not connected to registry (Offline?).", level="ERROR")
        return False
    # Basic validation for channel name and type
    if not channel_name or not channel_type or ' ' in channel_name:
        gui_log("ERROR] Invalid channel name (no spaces allowed) or type.", level="ERROR")
        return False
    if channel_type.lower() not in ['public', 'private']:
        gui_log("ERROR] Channel type must be 'public' or 'private'.", level="ERROR")
        return False

    try:
        msg = {
            'type': 'create_channel',
            'channel_name': channel_name,
            'channel_type': channel_type.lower(), # Send lowercase type
            'owner_name': MY_NAME,
            'owner_ip': MY_INFO.get('ip'),
            'owner_p2p_port': MY_INFO.get('p2p_port')
        }
        reg_socket.sendall((json.dumps(msg) + "\n").encode('utf-8'))
        gui_log(f"CMD] Channel creation request sent for '{channel_name}' ({channel_type}).")
        return True
    except (socket.error, OSError) as e:
        gui_log(f"ERROR] Network error sending create channel request: {e}", level="ERROR")
        return False
    except Exception as e:
        gui_log(f"ERROR] Failed to send create channel request: {e}", level="ERROR")
        return False

def join_channel(channel_name_to_join):
    """Sends P2P join request to channel owner. Assumes already online."""
    global available_channels_info, MY_NAME, MY_INFO, my_channels

    if is_offline:
        gui_log("JOIN ERROR] Cannot join channel while offline.", level="ERROR")
        return False
    if not channel_name_to_join:
        gui_log("JOIN ERROR] Channel name required.", level="ERROR")
        return False

    # Check if already in the channel
    with my_channels_lock:
        if channel_name_to_join in my_channels:
            role = "owner" if my_channels[channel_name_to_join].get('owner') == MY_NAME else "member"
            gui_log(f"JOIN INFO] Already a {role} of '{channel_name_to_join}'.")
            return True # Or False, as no action needed? True seems better.

    # Find owner info from available channels list
    owner_info = None
    with available_channels_info_lock:
        owner_info = available_channels_info.get(channel_name_to_join)

    if not owner_info:
        gui_log(f"JOIN ERROR] Channel '{channel_name_to_join}' not found in available list. Use 'List Channels' first.", level="ERROR")
        return False

    owner_name = owner_info.get('owner_name')
    owner_ip = owner_info.get('owner_ip')
    owner_port = owner_info.get('owner_p2p_port')

    if not owner_name or not owner_ip or not owner_port:
        gui_log(f"JOIN ERROR] Incomplete owner info for '{channel_name_to_join}' in available list.", level="ERROR")
        return False

    # Cannot join your own channel this way
    if owner_name == MY_NAME:
        gui_log(f"JOIN INFO] You are already the owner of '{channel_name_to_join}'.")
        # Add to my_channels if somehow missing? Should be added on creation.
        return True

    # Ensure own info is available for the request
    if not MY_INFO or not MY_NAME:
        gui_log("JOIN ERROR] Cannot join: Your own info (IP/Port) is missing.", level="ERROR")
        return False

    # Prepare the P2P join request message
    join_req_msg = {
        'type': 'request_join_channel',
        'channel_name': channel_name_to_join,
        'requester_name': MY_NAME,
        'requester_ip': MY_INFO.get('ip'),
        'requester_p2p_port': MY_INFO.get('p2p_port')
    }
    join_req_json = json.dumps(join_req_msg) + "\n"

    gui_log(f"JOIN] Sending P2P join request for '{channel_name_to_join}' to owner '{owner_name}'...")

    # Send the message P2P
    success = send_p2p_message(owner_name, join_req_json)

    if success:
        gui_log(f"JOIN] Request sent. Waiting for acceptance from {owner_name}.")
        # GUI should probably show some "pending" status? Or just wait for acceptance message.
    else:
        gui_log(f"JOIN ERROR] Failed to send P2P join request to owner '{owner_name}'. They might be offline or unreachable.", level="ERROR")

    return success


# --- Send Channel Message (Modified Output & Logic) ---
def send_channel_msg(channel_name, msg_content, reg_socket):
    """Sends message online (P2P/Server), or queues it if offline."""
    global my_channels, MY_NAME, MY_INFO, channel_message_logs, is_offline, offline_message_queue

    # Basic validation
    if not channel_name or not msg_content:
        gui_log("MSG ERROR] Channel name and message content cannot be empty.", level="ERROR")
        return False

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

        # Display locally immediately with indication (via GUI queue)
        try:
            ts_obj = datetime.datetime.fromisoformat(timestamp_iso.replace('Z', '+00:00'))
            local_ts = ts_obj.astimezone()
            time_str = local_ts.strftime('%H:%M:%S')
        except (ValueError, TypeError):
            time_str = "time?"

        # Format message for GUI display (indicating it's queued)
        formatted_msg = f"[{channel_name} @ {time_str}] {MY_NAME} (Offline Queued): {msg_content}"
        gui_queue.put(("CHANNEL_MSG", channel_name, formatted_msg))
        gui_log(f"MSG OFFLINE QUEUED] Message for '{channel_name}' queued for sending when online.")
        return True
    # --- End of Offline Handling ---

    # --- Online: Send immediately ---
    # Check if actually in the channel
    owner_name = None
    members_dict = {}
    is_owner = False
    with my_channels_lock:
        if channel_name not in my_channels:
            gui_log(f"MSG ERROR] Not a member of channel '{channel_name}'. Cannot send.", level="ERROR")
            return False
        channel_data = my_channels[channel_name]
        owner_name = channel_data.get('owner')
        members_dict = channel_data.get('members', {}).copy() # Get a copy
        is_owner = (owner_name == MY_NAME)

    if not MY_NAME or not MY_INFO:
        gui_log("MSG ERROR] Cannot send message: User identity info missing.", level="ERROR")
        return False

    timestamp_iso = datetime.datetime.now(timezone.utc).isoformat() # Timestamp for online send

    if is_owner:
        # --- Owner Sending (Online) ---
        gui_log(f"MSG SENDING] Broadcasting message as owner to '{channel_name}' members...")
        message_payload = {
            'type': 'channel_message',
            'channel_name': channel_name,
            'sender': MY_NAME,
            'content': msg_content,
            'timestamp': timestamp_iso
        }
        message_json = json.dumps(message_payload) + "\n"

        # 1. Log locally (Owner's log)
        with channel_logs_lock:
            log_entry = {'timestamp': timestamp_iso, 'sender': MY_NAME, 'content': msg_content}
            channel_message_logs.setdefault(channel_name, []).append(log_entry)
            # Optional: sort log?
            # Optional: notify GUI history updated?

        # 2. Display own message immediately (already done by P2P handler loopback? Maybe not needed here)
        # Let's rely on the P2P handler seeing the loopback or the registry notification.
        # If loopback P2P fails, registry 'notify_new_msg' should cover it.

        # 3. Broadcast P2P to all members (including self for loopback confirmation if listener works)
        send_count = 0
        for member_name in members_dict:
            # Should owner send to self via P2P? If P2P listener handles it right, yes.
            # If not, rely on registry notification. Let's send to self too for robustness.
             # if member_name != MY_NAME: # Optionally skip self
            threading.Thread(target=send_p2p_message, args=(member_name, message_json), daemon=True).start()
            send_count += 1
        gui_log(f"MSG SENT] Broadcast attempt to {send_count} members of '{channel_name}'.")

        # 4. Send to registry for storage/history/offline delivery
        if reg_socket:
            try:
                # Use the 'channel_message' payload for registry too
                registry_payload = message_payload.copy() # Make a copy just in case
                # Server might prefer a different type like 'log_message'? Assume 'channel_message' OK.
                registry_json = json.dumps(registry_payload) + "\n"
                reg_socket.sendall(registry_json.encode('utf-8'))
                gui_log(f"MSG SENT] Sent message for '{channel_name}' to registry.")
            except (socket.error, OSError) as e:
                gui_log(f"MSG ERROR] Failed to send message to registry: {e}", level="ERROR")
        else:
            gui_log("MSG WARN] Cannot send message to registry: Socket unavailable.", level="WARN")


    else:
        # --- Member Sending (Online) ---
        if not owner_name:
             # This shouldn't happen if we checked membership correctly, but safeguard
             gui_log(f"MSG ERROR] Cannot send message to '{channel_name}': Owner name unknown.", level="ERROR")
             return False

        gui_log(f"MSG SENDING] Forwarding message for '{channel_name}' to owner '{owner_name}'...")
        # Prepare 'forward_to_owner' message
        message_payload = {
            'type': 'forward_to_owner',
            'channel_name': channel_name,
            'sender': MY_NAME, # Original sender is us
            'content': msg_content,
            'timestamp': timestamp_iso
        }
        message_json = json.dumps(message_payload) + "\n"

        # 1. Attempt to send P2P to the owner
        p2p_success = send_p2p_message(owner_name, message_json)

        # 2. Display own message IF P2P forward was successful
        if p2p_success:
            gui_log(f"MSG SENT] Successfully forwarded message via P2P to owner '{owner_name}'.")
            # Display own message locally (via GUI queue)
            try:
                ts_obj = datetime.datetime.fromisoformat(timestamp_iso.replace('Z', '+00:00'))
                local_ts = ts_obj.astimezone()
                time_str = local_ts.strftime('%H:%M:%S')
            except (ValueError, TypeError):
                time_str = "time?"
            formatted_msg = f"[{channel_name} @ {time_str}] {MY_NAME}: {msg_content}"
            gui_queue.put(("CHANNEL_MSG", channel_name, formatted_msg))

        else:
            # P2P Failed - Try sending to registry as a fallback log mechanism
            gui_log(f"MSG WARNING] Failed P2P send to owner '{owner_name}'. Attempting to send to server log...", level="WARN")
            if reg_socket:
                try:
                    # Send the same 'forward_to_owner' message to the registry
                    reg_socket.sendall(message_json.encode('utf-8'))
                    gui_log("MSG INFO] Message forwarded to server log as P2P backup.")
                    # Display own message locally even if only server got it? Yes, seems reasonable.
                    try:
                        ts_obj = datetime.datetime.fromisoformat(timestamp_iso.replace('Z', '+00:00'))
                        local_ts = ts_obj.astimezone()
                        time_str = local_ts.strftime('%H:%M:%S')
                    except (ValueError, TypeError):
                        time_str = "time?"
                    formatted_msg = f"[{channel_name} @ {time_str}] {MY_NAME}: {msg_content}"
                    gui_queue.put(("CHANNEL_MSG", channel_name, formatted_msg))

                except (socket.error, OSError) as e:
                    gui_log(f"MSG ERROR] Failed to send message to server log after P2P failure: {e}", level="ERROR")
                    # Message likely lost if both P2P and registry fail
                    gui_log("MSG ERROR] Message likely not delivered.", level="ERROR")
                    return False # Indicate failure
            else:
                gui_log("MSG ERROR] Failed P2P to owner and cannot reach server (offline?). Message not sent.", level="ERROR")
                return False # Indicate failure

    return True # Indicate success (or at least successful attempt)


# --- Synchronization Functions (Modified Output) ---
def synchronize_host_server(reg_socket):
    """Sends owner's latest message timestamp for each owned channel to the server."""
    global channel_message_logs, my_channels, MY_NAME

    if is_offline or not reg_socket:
        gui_log("OWNER SYNC SERVER] Cannot sync: Offline or no registry socket.", level="WARN")
        return

    owned_channels_to_check = []
    # Get a list of owned channels safely
    with my_channels_lock:
        owned_channels_to_check = [
            name for name, data in my_channels.items()
            if data.get('owner') == MY_NAME
        ]

    if not owned_channels_to_check:
        gui_log("OWNER SYNC SERVER] No owned channels found to synchronize.")
        return

    gui_log(f"OWNER SYNC SERVER] Checking {len(owned_channels_to_check)} owned channels for synchronization...")

    for channel_name in owned_channels_to_check:
        content = []
        with channel_logs_lock:
            # Get a copy of the content list to avoid holding lock long
            content = channel_message_logs.get(channel_name, []).copy()
        if not content:
            gui_log(f"[OWNER SYNC SERVER] Owned channel '{channel_name}' has no local messages.")
            continue

        channel_latest_message = content[-1]
        channel_latest_timestamp = channel_latest_message.get('timestamp') 

        if channel_latest_timestamp:
            gui_log(f"[OWNER SYNC SERVER] Latest local message for owned channel '{channel_name}' (ts: {channel_latest_timestamp}):")
            gui_log(f"  -> {channel_latest_message}")

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
                gui_log(f"[SYNC SEND] Sent sync request for '{channel_name}' with timestamp {channel_latest_timestamp}")
            except Exception as e:
                gui_log(f"[SYNC ERROR] Failed to send sync request for '{channel_name}': {e}")

        else:
            gui_log(f"[OWNER SYNC SERVER WARN] Latest message in owned channel '{channel_name}' lacks a 'timestamp': {channel_latest_message}")

    gui_log("[OWNER SYNC SERVER] Finished checking owned channels.")


def synchronize_offline_messages(reg_socket):
    """Processes and attempts to send messages queued while offline."""
    global offline_message_queue, my_channels, channel_message_logs

    messages_to_process = []
    # Safely get and clear the queue
    with offline_queue_lock:
        if not offline_message_queue:
            # gui_log("[SYNC] No offline messages to synchronize.") # Can be noisy
            return # Nothing to do
        # Copy queue and clear original atomically
        messages_to_process = list(offline_message_queue)
        offline_message_queue.clear()

    gui_log(f"SYNC] Processing {len(messages_to_process)} queued offline message(s)...")

    if is_offline or not reg_socket:
        gui_log("SYNC ERROR] Cannot process offline queue: Client is still offline or registry unavailable.", level="ERROR")
        # Put messages back in queue? Or discard? Let's discard for now.
        # with offline_queue_lock:
        #     offline_message_queue.extend(messages_to_process) # Put them back if needed
        return

    successful_sends = 0
    failed_sends = 0

    for msg_data in messages_to_process:
        channel_name = msg_data.get('channel_name')
        content = msg_data.get('content')
        original_timestamp = msg_data.get('timestamp') # Use the timestamp from when it was typed

        if not channel_name or content is None or not original_timestamp:
            gui_log(f"SYNC ERROR] Invalid queued data skipped: {msg_data}", level="ERROR")
            failed_sends += 1
            continue

        # Re-check current role (owner/member) for the channel *now*
        owner_name = None
        members_dict = {}
        is_currently_owner = False
        is_currently_member = False # Need this flag

        with my_channels_lock:
            channel_info = my_channels.get(channel_name)
            if channel_info:
                is_currently_member = True # At least a member
                owner_name = channel_info.get('owner')
                members_dict = channel_info.get('members', {}).copy() # Get copy
                if owner_name == MY_NAME:
                    is_currently_owner = True
            else:
                 # Not currently in the channel anymore
                 gui_log(f"SYNC ERROR] Cannot send queued message for '{channel_name}': No longer in channel. Discarded.", level="ERROR")
                 failed_sends += 1
                 continue # Skip this message

        # --- Attempt to send based on current role ---
        message_sent = False
        if is_currently_owner:
            # --- Syncing Owner's Offline Message: Broadcast P2P & Send to Registry ---
            gui_log(f"SYNC] Sending queued owner message for '{channel_name}'...")
            message_payload = {
                'type': 'channel_message', # Standard broadcast type
                'channel_name': channel_name,
                'sender': MY_NAME,
                'content': content,
                'timestamp': original_timestamp # Use original timestamp
            }
            message_json = json.dumps(message_payload) + "\n"

            # Log locally first (might already be logged, but ensures it's there)
            # Duplication check might be needed here too
            with channel_logs_lock:
                log_entry = {'timestamp': original_timestamp, 'sender': MY_NAME, 'content': content}
                log_list = channel_message_logs.setdefault(channel_name, [])
                # Simple check: don't add if last message is identical (useful if queued multiple times)
                if not log_list or log_list[-1]['timestamp'] != original_timestamp or log_list[-1]['sender'] != MY_NAME or log_list[-1]['content'] != content:
                     log_list.append(log_entry)
                     # Sort?
                # else: gui_log(f"SYNC INFO] Duplicate offline owner message for {channel_name} avoided in log.")


            # Send P2P to all *current* members (including self for loopback)
            send_count = 0
            # We assume send_p2p_message handles target offline status correctly
            for member_name in members_dict:
                threading.Thread(target=send_p2p_message, args=(member_name, message_json), daemon=True).start()
                send_count += 1
            gui_log(f"SYNC] Broadcast attempt for queued message to {send_count} members of '{channel_name}'.")

            # Send to registry
            try:
                registry_payload = message_payload.copy()
                registry_json = json.dumps(registry_payload) + "\n"
                reg_socket.sendall(registry_json.encode('utf-8'))
                gui_log(f"SYNC] Sent queued owner message for '{channel_name}' to registry.")
                message_sent = True # Assume success if sent to registry
            except (socket.error, OSError) as e:
                gui_log(f"SYNC ERROR] Failed to send queued owner message to registry: {e}", level="ERROR")
                # Message might only reach online peers via P2P if this fails
                message_sent = False # Mark as potentially failed

        elif is_currently_member: # Must be member if not owner and check passed
             # --- Syncing Member's Offline Message: Forward P2P or Server ---
             if not owner_name: # Should have owner if member, but double check
                 gui_log(f"SYNC ERROR] Cannot send queued member message for '{channel_name}': Owner unknown. Discarded.", level="ERROR")
                 failed_sends += 1
                 continue

             gui_log(f"SYNC] Forwarding queued member message for '{channel_name}' to owner '{owner_name}'...")
             message_payload = {
                 'type': 'forward_to_owner', # Use forward type
                 'channel_name': channel_name,
                 'sender': MY_NAME, # We are the original sender
                 'content': content,
                 'timestamp': original_timestamp # Use original time
             }
             message_json = json.dumps(message_payload) + "\n"

             # Try P2P to owner first
             p2p_success = send_p2p_message(owner_name, message_json)

             if p2p_success:
                 gui_log(f"SYNC] Successfully forwarded queued message via P2P to owner '{owner_name}'.")
                 message_sent = True
             else:
                 # P2P failed, try registry backup
                 gui_log(f"SYNC WARNING] Failed P2P sync to owner '{owner_name}'. Sending to server log...", level="WARN")
                 try:
                     # Send the same 'forward_to_owner' message to registry
                     reg_socket.sendall(message_json.encode('utf-8'))
                     gui_log("SYNC INFO] Queued message forwarded to server log as P2P backup.")
                     message_sent = True # Count as success if registry gets it
                 except (socket.error, OSError) as e:
                     gui_log(f"SYNC ERROR] Failed to send queued message to server log: {e}. Discarded.", level="ERROR")
                     message_sent = False # Both failed

        # --- Update counters ---
        if message_sent:
            successful_sends += 1
        else:
            failed_sends += 1
            gui_log(f"SYNC ERROR] Failed to send queued message for '{channel_name}'.", level="ERROR")


    # End of loop
    gui_log(f"SYNC] Finished processing offline queue. Success: {successful_sends}, Failed/Discarded: {failed_sends}.")


# --- Login/Reconnect Functions (Modified for GUI Interaction) ---
def attempt_login_or_guest(host, port, login_details):
    """Handles connection and registry ack. Returns (success, message, socket)."""
    global MY_NAME, MY_ID, is_guest, MY_INFO, initial_login_info, registry_socket, MY_P2P_PORT

    login_type = login_details.get('type')
    name = login_details.get('name')
    user_id = login_details.get('id') # Will be None for guest

    # Validate inputs
    if not name or not (0 < len(name) <= 50) or ' ' in name:
        return False, "Invalid name (1-50 chars, no spaces).", None
    if login_type == 'user' and (not user_id or not user_id.isdigit()):
        return False, "Invalid ID (must be a number).", None

    # Prepare login data
    login_data = {}
    potential_login_info = {}
    update_my_info(name, MY_P2P_PORT) # Update MY_INFO with potential details

    if login_type == 'guest':
        MY_NAME = name
        MY_ID = None
        is_guest = True
        login_data = {'type': 'guest_login', 'name': MY_NAME, 'ip': MY_INFO.get('ip'), 'p2p_port': MY_P2P_PORT}
        potential_login_info = {'type': 'guest', 'name': MY_NAME}
    elif login_type == 'user':
        MY_NAME = name
        MY_ID = user_id
        is_guest = False
        login_data = {'type': 'user_login', 'name': MY_NAME, 'id': MY_ID, 'ip': MY_INFO.get('ip'), 'p2p_port': MY_P2P_PORT}
        potential_login_info = {'type': 'user', 'name': MY_NAME, 'id': MY_ID}
    else:
        return False, "Invalid login type specified.", None

    login_data['is_invisible'] = is_invisible # Include visibility state

    temp_socket = None
    try:
        gui_log(f"CONNECTING] Attempting connection to Registry {host}:{port}...")
        temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_socket.settimeout(10.0) # Connection timeout
        temp_socket.connect((host, port))
        gui_log(f"CONNECTED] Connection established. Sending identification as '{name}'...")

        # Send login data (append newline)
        temp_socket.sendall((json.dumps(login_data) + "\n").encode('utf-8'))
        gui_log(f"WAITING] Awaiting acknowledgement from registry...")

        # Wait for acknowledgement
        buffer = ""
        ack_received = False
        final_message = "No response from server."
        success = False
        temp_socket.settimeout(20.0) # Timeout for waiting for ack

        while not ack_received:
            try:
                data = temp_socket.recv(BUFFER_SIZE)
                if not data:
                    final_message = "Connection closed by server before acknowledgement."
                    ack_received = True # Treat as received (failed)
                    break

                buffer += data.decode('utf-8')

                # Process if newline present
                if '\n' in buffer:
                    resp_json, buffer = buffer.split('\n', 1) # Process one message
                    response = json.loads(resp_json)
                    r_type = response.get('type')
                    ack_received = True # Got a recognizable response

                    if r_type == 'login_ack' or r_type == 'guest_ack':
                        final_message = response.get('message', "Login successful!")
                        success = True
                        # Update IP if provided by server
                        server_ip = response.get('client_ip')
                        if server_ip:
                            update_my_info(MY_NAME, MY_P2P_PORT, ip=server_ip)

                        # Success! Keep the socket and store login info
                        registry_socket = temp_socket
                        registry_socket.settimeout(None) # Remove timeout for listener
                        initial_login_info = potential_login_info
                        return True, final_message, registry_socket

                    elif r_type == 'error':
                        final_message = response.get('message', "Login failed by server.")
                        success = False
                        # Close socket on error
                        if temp_socket: temp_socket.close(); temp_socket = None
                        return False, final_message, None
                    else:
                        final_message = f"Unexpected response type from server: {r_type}"
                        success = False
                        # Close socket on unexpected response
                        if temp_socket: temp_socket.close(); temp_socket = None
                        return False, final_message, None

            except socket.timeout:
                final_message = "Timeout waiting for server acknowledgement."
                ack_received = True # Stop waiting
                break
            except (socket.error, json.JSONDecodeError, UnicodeDecodeError) as e:
                final_message = f"Error reading/decoding acknowledgement: {e}"
                ack_received = True
                break
            except Exception as e:
                 final_message = f"Unexpected error during acknowledgement: {e}"
                 ack_received = True
                 break

        # If loop finished without success
        if temp_socket:
            try: temp_socket.close()
            except: pass
        return False, final_message, None

    except socket.timeout:
        error_msg = f"Connection to registry {host}:{port} timed out."
        gui_log(error_msg, level="ERROR")
        if temp_socket: temp_socket.close()
        return False, error_msg, None
    except socket.error as e:
        error_msg = f"Registry connection error: {e}"
        gui_log(error_msg, level="ERROR")
        if temp_socket: temp_socket.close()
        return False, error_msg, None
    except Exception as e:
        error_msg = f"Critical login error: {e}"
        gui_log(error_msg, level="CRITICAL")
        if temp_socket: temp_socket.close()
        running = False # Stop the app on critical errors?
        return False, error_msg, None


def attempt_reconnect_and_register(host, port):
    """Attempts to reconnect using stored initial_login_info."""
    global registry_socket, MY_INFO, initial_login_info, running, MY_NAME, MY_ID, is_guest, MY_P2P_PORT

    if not initial_login_info:
        return False, "Cannot reconnect: Initial login info missing.", None

    # Retrieve stored details
    login_type = initial_login_info.get('type')
    name = initial_login_info.get('name')
    user_id = initial_login_info.get('id') # None for guest

    # Prepare login data based on stored info
    login_data = {}
    update_my_info(name, MY_P2P_PORT) # Update MY_INFO before sending

    if login_type == 'guest' and name:
        login_data = {'type': 'guest_login', 'name': name, 'ip': MY_INFO.get('ip'), 'p2p_port': MY_P2P_PORT}
        # Update globals if needed (should be set already, but safety)
        MY_NAME = name; MY_ID = None; is_guest = True;
    elif login_type == 'user' and name and user_id:
        login_data = {'type': 'user_login', 'name': name, 'id': user_id, 'ip': MY_INFO.get('ip'), 'p2p_port': MY_P2P_PORT}
        MY_NAME = name; MY_ID = user_id; is_guest = False;
    else:
        return False, "Cannot reconnect: Invalid stored login info.", None

    login_data['is_invisible'] = is_invisible # Send current visibility state

    temp_socket = None
    try:
        gui_log(f"RECONNECTING] Attempting reconnection to Registry {host}:{port}...")
        temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_socket.settimeout(10.0) # Connection timeout
        temp_socket.connect((host, port))
        gui_log("[CONNECTED] Reconnection established. Sending identification...")

        # Send identification
        temp_socket.sendall((json.dumps(login_data) + "\n").encode('utf-8'))
        gui_log("[WAITING] Awaiting re-login acknowledgement...")

        # Wait for acknowledgement (similar logic to initial login)
        buffer = ""; ack_received = False; final_message = "No reconnect response."; success = False
        temp_socket.settimeout(20.0) # Ack timeout

        while not ack_received:
            try:
                data = temp_socket.recv(BUFFER_SIZE)
                if not data:
                    final_message = "Connection closed by server during reconnect ack."
                    ack_received = True; break
                buffer += data.decode('utf-8')
                if '\n' in buffer:
                    resp_json, buffer = buffer.split('\n', 1)
                    response = json.loads(resp_json)
                    r_type = response.get('type')
                    ack_received = True

                    if r_type == 'login_ack' or r_type == 'guest_ack':
                        final_message = response.get('message', "Reconnection successful!")
                        success = True
                        server_ip = response.get('client_ip')
                        if server_ip:
                            update_my_info(name, MY_P2P_PORT, ip=server_ip)
                        # Assign the new socket
                        registry_socket = temp_socket
                        registry_socket.settimeout(None) # Ready for listener
                        return True, final_message, registry_socket
                    elif r_type == 'error':
                        final_message = f"Reconnect failed: {response.get('message', 'Server error')}"
                        success = False; break # Exit loop on error
                    else:
                        final_message = f"Unexpected ack type during reconnect: {r_type}"
                        success = False; break
            except socket.timeout:
                final_message = "Timeout waiting for reconnect acknowledgement."
                ack_received = True; break
            except (socket.error, json.JSONDecodeError, UnicodeDecodeError) as e:
                final_message = f"Error reading/decoding reconnect ack: {e}"
                ack_received = True; break
            except Exception as e:
                final_message = f"Unexpected error during reconnect ack: {e}"
                ack_received = True; break

        # Cleanup if loop finished without success
        if temp_socket:
            try: temp_socket.close()
            except: pass
        registry_socket = None # Ensure socket is None if failed
        return False, final_message, None

    except socket.timeout:
        error_msg = f"Connection timed out during reconnect attempt to {host}:{port}."
        gui_log(error_msg, level="ERROR")
        if temp_socket: temp_socket.close()
        return False, error_msg, None
    except socket.error as e:
        error_msg = f"Reconnect connection error: {e}"
        gui_log(error_msg, level="ERROR")
        if temp_socket: temp_socket.close()
        return False, error_msg, None
    except Exception as e:
        error_msg = f"Critical reconnect error: {e}"
        gui_log(error_msg, level="CRITICAL")
        if temp_socket: temp_socket.close()
        return False, error_msg, None


# --- Tkinter GUI Application ---
class P2PClientGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(f"P2P Chat Client (Port: {MY_P2P_PORT})")
        self.geometry("800x600")
        self.protocol("WM_DELETE_WINDOW", self._on_closing)

        # Style
        self.default_font = tkfont.nametofont("TkDefaultFont")
        self.default_font.configure(size=10)
        self.option_add("*Font", self.default_font)

        # State
        self.current_channel = None # Track selected channel for messaging
        # *** NEW: Dictionary to store messages per channel for display ***
        self.channel_display_buffers = {} # {channel_name: [formatted_msg1, ...]}
        self.channel_notification_state  = {} # {channel_name: bool}
        # GUI Elements
        self._create_widgets() # Creates chat_area among others

        # Start with login window
        self._show_login_dialog()

        # Start processing the GUI queue
        self.after(100, self._process_gui_queue)

    def _create_widgets(self):
        # Main frame
        main_frame = tk.Frame(self)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        main_frame.columnconfigure(1, weight=1) # Allow chat area to expand
        main_frame.rowconfigure(0, weight=1) # Allow chat area to expand

        # --- Left Pane (Lists) ---
        left_pane = tk.Frame(main_frame, width=200)
        left_pane.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        left_pane.rowconfigure(1, weight=1) # Peers list expands
        left_pane.rowconfigure(3, weight=1) # My channels list expands
        left_pane.rowconfigure(5, weight=1) # Available channels list expands

        tk.Label(left_pane, text="Peers Online:").grid(row=0, column=0, sticky="w")
        self.peer_listbox = tk.Listbox(left_pane, height=8, exportselection=False)
        self.peer_listbox.grid(row=1, column=0, sticky="nsew")
        peer_scroll = tk.Scrollbar(left_pane, orient=tk.VERTICAL, command=self.peer_listbox.yview)
        peer_scroll.grid(row=1, column=1, sticky="ns")
        self.peer_listbox.configure(yscrollcommand=peer_scroll.set)
        # Add binding? e.g., double click to start private chat (not implemented)

        tk.Label(left_pane, text="My Channels:").grid(row=2, column=0, sticky="w", pady=(10, 0))
        self.my_channels_listbox = tk.Listbox(left_pane, height=6, exportselection=False)
        self.my_channels_listbox.grid(row=3, column=0, sticky="nsew")
        my_channels_scroll = tk.Scrollbar(left_pane, orient=tk.VERTICAL, command=self.my_channels_listbox.yview)
        my_channels_scroll.grid(row=3, column=1, sticky="ns")
        self.my_channels_listbox.configure(yscrollcommand=my_channels_scroll.set)
        self.my_channels_listbox.bind('<<ListboxSelect>>', self._on_my_channel_select)

        tk.Label(left_pane, text="Available Channels:").grid(row=4, column=0, sticky="w", pady=(10, 0))
        self.avail_channels_listbox = tk.Listbox(left_pane, height=6, exportselection=False)
        self.avail_channels_listbox.grid(row=5, column=0, sticky="nsew")
        avail_channels_scroll = tk.Scrollbar(left_pane, orient=tk.VERTICAL, command=self.avail_channels_listbox.yview)
        avail_channels_scroll.grid(row=5, column=1, sticky="ns")
        self.avail_channels_listbox.configure(yscrollcommand=avail_channels_scroll.set)
        self.avail_channels_listbox.bind('<Double-Button-1>', self._on_avail_channel_doubleclick) # Double click to join

        # --- Right Pane (Chat and Input) ---
        right_pane = tk.Frame(main_frame)
        right_pane.grid(row=0, column=1, sticky="nsew")
        right_pane.rowconfigure(0, weight=1) # Chat area expands
        right_pane.columnconfigure(0, weight=1) # Chat area expands

        # Chat display area
        self.chat_area = scrolledtext.ScrolledText(right_pane, wrap=tk.WORD, state='disabled', height=20)
        self.chat_area.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=(0, 5))
        # Configure tags for different message types
        self.chat_area.tag_configure("INFO", foreground="blue") # Might not be used in chat anymore
        self.chat_area.tag_configure("ERROR", foreground="red")
        self.chat_area.tag_configure("WARN", foreground="orange")
        self.chat_area.tag_configure("CRITICAL", foreground="red", font=(self.default_font.actual()['family'], self.default_font.actual()['size'], 'bold'))
        self.chat_area.tag_configure("CHANNEL_MSG", foreground="black") # Default for channel messages
        self.chat_area.tag_configure("SELF_MSG", foreground="#008800") # Darker Green for own messages
        # *** ADDED TAG for channel status messages ***
        self.chat_area.tag_configure("JOIN_PART", foreground="gray", font=(self.default_font.actual()['family'], self.default_font.actual()['size'], 'italic'))
        self.chat_area.tag_configure("SYSTEM", foreground="purple") # Optional for system messages shown


        # Input field
        self.input_var = tk.StringVar()
        self.input_entry = tk.Entry(right_pane, textvariable=self.input_var)
        self.input_entry.grid(row=1, column=0, sticky="ew", padx=(0, 5))
        self.input_entry.bind("<Return>", self._send_message_or_command)

        # Send button
        self.send_button = tk.Button(right_pane, text="Send", command=self._send_message_or_command, width=10)
        self.send_button.grid(row=1, column=1, sticky="e")

        # --- Bottom Pane (Buttons and Status) ---
        bottom_pane = tk.Frame(self)
        bottom_pane.pack(side=tk.BOTTOM, fill=tk.X, padx=5, pady=(0, 5))

        button_frame = tk.Frame(bottom_pane)
        button_frame.pack(side=tk.LEFT)

        self.list_peers_button = tk.Button(button_frame, text="List Peers", command=self._list_peers)
        self.list_peers_button.pack(side=tk.LEFT, padx=2)
        self.list_channels_button = tk.Button(button_frame, text="List Channels", command=self._list_channels)
        self.list_channels_button.pack(side=tk.LEFT, padx=2)
        self.create_channel_button = tk.Button(button_frame, text="Create", command=self._create_channel_dialog)
        self.create_channel_button.pack(side=tk.LEFT, padx=2)
        self.join_channel_button = tk.Button(button_frame, text="Join", command=self._join_channel_dialog)
        self.join_channel_button.pack(side=tk.LEFT, padx=2)
        self.visibility_button = tk.Button(button_frame, text="Go Invisible", command=self._toggle_visibility)
        self.visibility_button.pack(side=tk.LEFT, padx=2)
        self.online_offline_button = tk.Button(button_frame, text="Go Online", command=self._toggle_online_offline)
        self.online_offline_button.pack(side=tk.LEFT, padx=2)

        # Status Bar
        self.status_var = tk.StringVar()
        self.status_bar = tk.Label(bottom_pane, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=(10, 0))
        self._update_status_bar() # Initial status

        # Initially disable most buttons until logged in/online
        self._set_ui_state(logged_in=False, online=False)


    def _set_ui_state(self, logged_in, online):
        """Enable/disable UI elements based on login and online status."""
        # State for elements requiring online connection
        online_state = tk.NORMAL if (logged_in and online) else tk.DISABLED
        # State for elements requiring only login (can be done offline)
        logged_in_state = tk.NORMAL if logged_in else tk.DISABLED

        # *** Input and Send Button: Require logged_in AND a selected channel ***
        # *** Now allows sending when offline ***
        can_type = logged_in and self.current_channel is not None
        self.input_entry.config(state=tk.NORMAL if can_type else tk.DISABLED)
        self.send_button.config(state=tk.NORMAL if can_type else tk.DISABLED)

        # Buttons requiring online connection (remain the same)
        self.list_peers_button.config(state=online_state)
        self.list_channels_button.config(state=online_state)
        self.create_channel_button.config(state=online_state)
        self.join_channel_button.config(state=online_state)

        # Buttons requiring only login (remain the same)
        self.visibility_button.config(state=logged_in_state)
        self.online_offline_button.config(state=logged_in_state)

        # Configure Online/Offline button text (remains the same)
        self.online_offline_button.config(text="Go Offline" if online else "Go Online")

        # Clear lists when going offline (remains the same)
        if not online:
            self.peer_listbox.delete(0, tk.END)
            self.peer_listbox.insert(tk.END, "(Offline)")
            self.avail_channels_listbox.delete(0, tk.END)
            self.avail_channels_listbox.insert(tk.END, "(Offline)")
             # Keep 'My Channels' listbox populated

        # Ensure input is disabled if no channel selected, even if logged in
        if not self.current_channel:
            self.input_entry.config(state=tk.DISABLED)
            self.send_button.config(state=tk.DISABLED)

        # Set focus if possible
        if can_type and not online: # If offline but can type, focus input
            self.input_entry.focus_set()
        elif can_type and online: # If online and can type, focus input
            self.input_entry.focus_set()


    def _show_login_dialog(self):
        """Displays the initial login dialog."""
        self.login_window = tk.Toplevel(self)
        self.login_window.title("Login")
        self.login_window.geometry("300x180")
        self.login_window.resizable(False, False)
        self.login_window.transient(self) # Keep on top of main window
        self.login_window.grab_set() # Modal

        # Center the login window
        self.update_idletasks()
        parent_x = self.winfo_rootx()
        parent_y = self.winfo_rooty()
        parent_width = self.winfo_width()
        parent_height = self.winfo_height()
        dialog_width = self.login_window.winfo_width()
        dialog_height = self.login_window.winfo_height()
        x = parent_x + (parent_width // 2) - (dialog_width // 2)
        y = parent_y + (parent_height // 2) - (dialog_height // 2)
        self.login_window.geometry(f"+{x}+{y}")


        frame = tk.Frame(self.login_window, padx=10, pady=10)
        frame.pack(expand=True, fill=tk.BOTH)

        tk.Label(frame, text="Username:").grid(row=0, column=0, sticky="w", pady=2)
        self.name_entry = tk.Entry(frame, width=30)
        self.name_entry.grid(row=0, column=1, pady=2)

        tk.Label(frame, text="User ID (Login only):").grid(row=1, column=0, sticky="w", pady=2)
        self.id_entry = tk.Entry(frame, width=30)
        self.id_entry.grid(row=1, column=1, pady=2)

        self.login_status_var = tk.StringVar()
        tk.Label(frame, textvariable=self.login_status_var, fg="red").grid(row=2, column=0, columnspan=2, pady=5)

        button_frame = tk.Frame(frame)
        button_frame.grid(row=3, column=0, columnspan=2, pady=10)

        guest_button = tk.Button(button_frame, text="Login as Guest", command=self._handle_guest_login)
        guest_button.pack(side=tk.LEFT, padx=10)
        login_button = tk.Button(button_frame, text="Login", command=self._handle_user_login)
        login_button.pack(side=tk.LEFT, padx=10)

        self.login_window.protocol("WM_DELETE_WINDOW", self._quit_app) # Close app if login cancelled

        self.name_entry.focus_set()

    def _handle_guest_login(self):
        name = self.name_entry.get().strip()
        if not name:
            self.login_status_var.set("Guest name cannot be empty.")
            return
        self.login_status_var.set("Attempting guest login...")
        login_details = {'type': 'guest', 'name': name}
        # Run login attempt in a separate thread to avoid blocking GUI
        threading.Thread(target=self._execute_login, args=(login_details,), daemon=True).start()

    def _handle_user_login(self):
        name = self.name_entry.get().strip()
        user_id = self.id_entry.get().strip()
        if not name or not user_id:
            self.login_status_var.set("Username and ID required for login.")
            return
        self.login_status_var.set("Attempting user login...")
        login_details = {'type': 'user', 'name': name, 'id': user_id}
        # Run login attempt in a separate thread
        threading.Thread(target=self._execute_login, args=(login_details,), daemon=True).start()

    def _execute_login(self, login_details):
        """Called in a thread to perform the actual login network operations."""
        global is_offline
        success, message, sock = attempt_login_or_guest(REGISTRY_HOST, REGISTRY_PORT, login_details)

        # Use queue to update GUI from this thread
        gui_queue.put(("LOGIN_RESULT", success, message))
        if success:
            # Start listeners only after successful login
            is_offline = False # Go online state
            self._start_network_listeners(sock)
            gui_queue.put("LOGIN_SUCCESS") # Signal GUI main thread
            # Request initial lists after successful login
            self._request_initial_data(sock)


    def _request_initial_data(self, sock):
        """Request peer and channel lists after login."""
        if sock and not is_offline:
            try:
                # Request peers
                peer_req = {'type': 'list'} # Server might need ID, adjust if needed
                sock.sendall((json.dumps(peer_req) + "\n").encode('utf-8'))
                # Request channels
                chan_req = {'type': 'request_channel_list'}
                sock.sendall((json.dumps(chan_req) + "\n").encode('utf-8'))
                gui_log("Requested initial peer and channel lists.")
            except Exception as e:
                gui_log(f"Error requesting initial data: {e}", level="ERROR")


    def _handle_login_result(self, success, message):
        """Updates login dialog based on result from network thread."""
        self.login_status_var.set(message)
        if success:
            self.login_window.destroy() # Close login dialog
            self.log_message(f"Login successful as {MY_NAME}.")
            self._update_status_bar()
            self._set_ui_state(logged_in=True, online=True) # Update main UI
        else:
            # Keep login dialog open on failure
            messagebox.showerror("Login Failed", message)
            self.login_status_var.set("") # Clear status for next attempt


    def _start_network_listeners(self, reg_sock):
        """Starts the P2P and Registry listener threads."""
        global p2p_listener_thread, registry_listener_thread, stop_p2p_listener_event, stop_registry_listener_event

        # Ensure previous threads are stopped if any (e.g., after reconnect)
        self._stop_listeners()

        stop_p2p_listener_event = threading.Event()
        stop_registry_listener_event = threading.Event()

        self.log_message(f"Starting P2P listener on {LISTEN_HOST}:{MY_P2P_PORT}...")
        p2p_listener_thread = threading.Thread(target=listen_for_peers, args=(LISTEN_HOST, MY_P2P_PORT, stop_p2p_listener_event), daemon=True)
        p2p_listener_thread.start()

        time.sleep(0.1) # Small delay

        if reg_sock:
            self.log_message("Starting Registry listener...")
            registry_listener_thread = threading.Thread(target=listen_to_registry, args=(reg_sock, stop_registry_listener_event), daemon=True)
            registry_listener_thread.start()
        else:
            self.log_message("CRITICAL: Registry socket unavailable after login. Forcing offline.", level="CRITICAL")
            # Handle this state - maybe trigger offline mode immediately
            self._go_offline_logic(notify_server=False)


    def _stop_listeners(self):
        """Signals listener threads to stop and waits briefly."""
        global p2p_listener_thread, registry_listener_thread, registry_socket

        stopped = False
        if 'stop_p2p_listener_event' in globals() and stop_p2p_listener_event:
            stop_p2p_listener_event.set()
            stopped = True
        if 'stop_registry_listener_event' in globals() and stop_registry_listener_event:
            stop_registry_listener_event.set()
            stopped = True

        if stopped:
             self.log_message("Stopping network listeners...")
             # Give threads a moment to exit cleanly - Adjust time as needed
             # Joining might block GUI, so maybe don't join here or do it carefully
             # time.sleep(0.2) # Be cautious with sleeps in GUI thread

        # Clear thread variables
        p2p_listener_thread = None
        registry_listener_thread = None


    def _process_gui_queue(self):
        """Processes messages from the background threads."""
        try:
            while True: # Process all messages currently in queue
                item = gui_queue.get_nowait()

                if isinstance(item, str):
                    # Infer level from the string content for basic logging
                    level = "INFO" # Default assumption for plain strings
                    msg_lower = item.lower()
                    if msg_lower.startswith("[error") or msg_lower.startswith("error"): level = "ERROR"
                    elif msg_lower.startswith("[warn") or msg_lower.startswith("warn"): level = "WARN"
                    elif msg_lower.startswith("[critical") or msg_lower.startswith("critical"): level = "CRITICAL"
                    elif msg_lower.startswith("[channel") or "joined channel" in msg_lower or "left channel" in msg_lower: level = "JOIN_PART"
                    elif msg_lower.startswith("[info") or msg_lower.startswith("info"): level = "INFO"
                    # Add more specific checks if needed

                    self.log_message(item, level=level) # Pass inferred level


                elif isinstance(item, tuple):
                    msg_type = item[0]
                     # --- Handle structured messages ---
                    if msg_type == "CHANNEL_MSG":
                        # Tuple format: ("CHANNEL_MSG", channel_name, formatted_message_string)
                        channel, formatted_msg = item[1], item[2]
                        # This function now handles buffering and conditional display
                        self._display_channel_message(channel, formatted_msg)

                    # --- Handle new message notification ---
                    elif msg_type == "NEW_MESSAGE_NOTICE":
                        channel_name = item[1]
                        self._trigger_notification_update(channel_name)

                    # --- Other tuple handlers remain the same ---
                    elif msg_type == "UPDATE_PEERS":
                        peers_data = item[1]
                        self._update_peer_list(peers_data)
                    elif msg_type == "UPDATE_MY_CHANNELS":
                        my_channels_data = item[1]
                        # When my channels update, clear buffers for channels no longer part of?
                        # Or maybe just clear chat if current channel removed? Simpler to leave buffers.
                        self._update_my_channels_list(my_channels_data)
                        # Check if current channel is still valid
                        if self.current_channel and self.current_channel not in my_channels_data:
                            self.log_message(f"You are no longer in channel '{self.current_channel}'.", level="WARN")
                            self.current_channel = None
                            self.chat_area.config(state='normal')
                            self.chat_area.delete('1.0', tk.END)
                            self.chat_area.insert(tk.END, "Select a channel from 'My Channels'.\n", "SYSTEM")
                            self.chat_area.config(state='disabled')
                            self._set_ui_state(logged_in=True, online=(not is_offline)) # Update button states
                            self._update_status_bar()


                    elif msg_type == "UPDATE_AVAILABLE_CHANNELS":
                        avail_channels_data = item[1]
                        self._update_available_channels_list(avail_channels_data)
                    elif msg_type == "UPDATE_STATUS":
                        name, offline_stat, invis_stat = item[1], item[2], item[3]
                        self._update_status_bar(name, offline_stat, invis_stat)
                    elif msg_type == "LOGIN_RESULT":
                        success, message = item[1], item[2]
                        self._handle_login_result(success, message)
                    elif msg_type == "RECONNECT_RESULT":
                         success, message = item[1], item[2]
                         if not success:
                             messagebox.showerror("Reconnect Failed", message)
                             # Ensure button is re-enabled on failure
                             self.online_offline_button.config(state=tk.NORMAL)
                         else:
                             self.log_message(message, level="SYSTEM") # Log success as system message
                         self._update_status_bar()
                         self._set_ui_state(logged_in=True, online=success)
                         # Re-enable button after attempt regardless of success/failure
                         self.online_offline_button.config(state=tk.NORMAL if initial_login_info else tk.DISABLED)

                    elif msg_type == "LOGIN_SUCCESS":
                         self.log_message(f"Welcome, {'Guest ' if is_guest else ''}{MY_NAME}!", level="SYSTEM")
                         self._update_status_bar()
                         self._set_ui_state(logged_in=True, online=True)
                    elif msg_type == "FORCE_OFFLINE":
                         reason = item[1]
                         self.log_message(f"Forced offline: {reason}", level="WARN")
                         if not is_offline:
                              self._go_offline_logic(notify_server=False)
                    # --- Handle potential raw log tuples if needed ---
                    # elif msg_type == "LOG":
                    #    level, message = item[1], item[2]
                    #    self.log_message(message, level=level)

                    else:
                        # Log unknown tuple types to console for debugging
                        print(f"[DEBUG] Unknown GUI queue tuple: {item}")

        except queue.Empty:
            pass # No messages left in queue
        except Exception as e:
            # Log errors during queue processing to console and maybe GUI error log
            print(f"CRITICAL Error processing GUI queue: {e}")
            import traceback
            traceback.print_exc()
            self.log_message(f"CRITICAL Error processing GUI events: {e}", level="CRITICAL")

        # Reschedule the check
        self.after(100, self._process_gui_queue)

    

    def log_message(self, msg, level="INFO"):
        """Appends a message to the chat area with appropriate tag."""
        # Ensure GUI updates happen in the main thread
        if threading.current_thread() != threading.main_thread():
             # If called from background thread, put it back into queue properly
             gui_queue.put(f"[{level}] {msg}")
             return

        try:
            tag = level # Use level as tag name ("INFO", "ERROR", etc.)
            self.chat_area.config(state='normal')
            self.chat_area.insert(tk.END, msg + '\n', tag)
            self.chat_area.config(state='disabled')
            self.chat_area.see(tk.END) # Auto-scroll
        except Exception as e:
            print(f"Error displaying log message in GUI: {e}") # Fallback

    def _trigger_notification_update(self, channel_name):
        """Sets notification state if channel is not active and redraws list."""
        # Don't notify if user is already looking at the channel
        if channel_name == self.current_channel:
            return

        # Don't notify if user is not currently in the channel anymore
        with my_channels_lock:
            if channel_name not in my_channels:
                return

        # Set the notification state
        self.channel_notification_state[channel_name] = True

        # Redraw the 'My Channels' list to show the indicator
        # We call the existing update function which now reads the state
        with my_channels_lock: # Get current channels to pass to update function
            current_my_channels = my_channels.copy()
        self._update_my_channels_list(current_my_channels)

    def _display_channel_message(self, channel, formatted_msg):
         """Buffers channel messages and displays only if channel is active."""
         # 1. Buffer the message regardless of the current channel
         # Ensure the buffer list exists for this channel
         buffer = self.channel_display_buffers.setdefault(channel, [])
         buffer.append(formatted_msg) # Add the new message

         # 2. Display the message ONLY if it's for the currently selected channel
         if channel == self.current_channel:
             tag = "CHANNEL_MSG" # Default tag
             # Detect if it's the user's own message based on format
             if f"] {MY_NAME} " in formatted_msg and \
                (formatted_msg.strip().endswith(":") or f"] {MY_NAME} (You):" in formatted_msg or f"] {MY_NAME} (Offline Queued):" in formatted_msg):
                 tag = "SELF_MSG"

             self.chat_area.config(state='normal')
             # Check if message already ends with newline
             if not formatted_msg.endswith('\n'):
                 formatted_msg += '\n'
             self.chat_area.insert(tk.END, formatted_msg, tag)
             self.chat_area.config(state='disabled')
             self.chat_area.see(tk.END)

    def _update_status_bar(self, name=None, offline=None, invisible=None):
        """Updates the status bar text."""
        # Use global state if arguments not provided
        current_name = name if name is not None else MY_NAME
        current_offline = offline if offline is not None else is_offline
        current_invisible = invisible if invisible is not None else is_invisible

        status = current_name if current_name else "Not Logged In"
        status += " | " + ("OFFLINE" if current_offline else "ONLINE")
        if not current_offline: # Only show visibility if online
            status += " (" + ("Invisible" if current_invisible else "Visible") + ")"
        if self.current_channel:
             status += f" | Channel: {self.current_channel}"
        self.status_var.set(status)


    def _update_peer_list(self, peers_data):
        """Updates the peer listbox."""
        self.peer_listbox.delete(0, tk.END)
        sorted_peers = sorted(peers_data.values(), key=lambda p: p.get('name', 'z').lower())
        for p_info in sorted_peers:
            name = p_info.get('name', '?')
            p_ip = p_info.get('ip', '?')
            p_port = p_info.get('p2p_port', '?')
            # Omit self from list
            if name == MY_NAME: continue
            # Omit invisible peers
            if p_info.get('is_invisible', False): continue

            offline_status = "(Offline)" if p_info.get('is_offline', False) else ""
            entry = f"{name} {offline_status}" # ({p_ip}:{p_port})" # Keep it simple
            self.peer_listbox.insert(tk.END, entry)


    # --- Modified _update_my_channels_list with itemconfig for color ---
    def _update_my_channels_list(self, my_channels_data):
        """Updates the 'My Channels' listbox, adding notification markers and color."""
        current_selection = self.my_channels_listbox.curselection()
        selected_channel = None
        if current_selection:
            try:
                selected_index = current_selection[0]
                # *** Get text and strip marker before extracting name ***
                selected_text = self.my_channels_listbox.get(selected_index).lstrip("* ")
                selected_channel = selected_text.split(" (")[0]
            except Exception as e:
                print(f"[WARN] Error getting selected channel name during update: {e}")
                selected_channel = None

        # Store scroll position before clearing
        scroll_pos = self.my_channels_listbox.yview()

        # Clear the listbox before rebuilding
        self.my_channels_listbox.delete(0, tk.END)

        sorted_channels = sorted(my_channels_data.items())
        new_selection_index = -1
        idx = 0 # Keep track of the index for itemconfig

        # Define colors
        default_fg_color = 'black' # Or query system default if needed
        notification_fg_color = 'red'

        for name, data in sorted_channels:
            role = "Owner" if data.get('owner') == MY_NAME else "Member"
            owner_display = f" (Owner: {data.get('owner', '?')})" if role == "Member" else ""
            member_count = len(data.get('members', {}))
            count_display = f", {member_count} members" if role == "Owner" else ""

            # Check notification state
            has_notification = self.channel_notification_state.get(name, False)
            notification_marker = "* " if has_notification else ""

            # Construct the entry text
            entry_text = f"{notification_marker}{name}"
            if role == "Member":
                 entry_text += owner_display # Append owner only for members

            # Insert the item text using the current index
            self.my_channels_listbox.insert(idx, entry_text)

            # *** Configure foreground color based on notification state ***
            item_color = notification_fg_color if has_notification else default_fg_color
            self.my_channels_listbox.itemconfig(idx, {'foreground': item_color})
            # Optionally, configure font weight here too if desired
            # item_weight = 'bold' if has_notification else 'normal'
            # self.my_channels_listbox.itemconfig(idx, {'foreground': item_color, 'font': (self.default_font.actual()['family'], self.default_font.actual()['size'], item_weight)})

            # Check if this item should be re-selected
            if name == selected_channel:
                 new_selection_index = idx

            idx += 1 # Increment index for the next item

        # Re-select previously selected channel if still present
        if new_selection_index != -1:
            try:
                self.my_channels_listbox.selection_set(new_selection_index)
                self.my_channels_listbox.activate(new_selection_index)
                # Optional: Ensure the selection is visible
                # self.my_channels_listbox.see(new_selection_index)
            except tk.TclError as e:
                 # Handle potential errors if index is invalid after update (shouldn't happen often)
                 print(f"[WARN] Error re-selecting item at index {new_selection_index}: {e}")


        # Restore scroll position
        self.my_channels_listbox.yview_moveto(scroll_pos[0])




    def _update_available_channels_list(self, avail_channels_data):
        """Updates the 'Available Channels' listbox."""
        self.avail_channels_listbox.delete(0, tk.END)
        sorted_channels = sorted(avail_channels_data.items())
        for name, info in sorted_channels:
            owner = info.get('owner_name', 'N/A')
            ctype = info.get('channel_type', 'N/A')
            # Don't list channels the user is already in? Optional.
            # with my_channels_lock:
            #    if name in my_channels:
            #        continue
            entry = f"{name} (Owner: {owner}, Type: {ctype})"
            self.avail_channels_listbox.insert(tk.END, entry)


    # --- Modify _on_my_channel_select ---
    # --- Corrected _on_my_channel_select ---
    def _on_my_channel_select(self, event):
         """Handles selection change. Clears notification and loads chat."""
         selection = self.my_channels_listbox.curselection()
         if not selection:
             # Handling for no selection (or deselection)
             if self.current_channel is not None: # Only update if a channel was previously selected
                 self.current_channel = None
                 self.chat_area.config(state='normal')
                 self.chat_area.delete('1.0', tk.END)
                 self.chat_area.insert(tk.END, "Select a channel from 'My Channels'.\n", "SYSTEM")
                 self.chat_area.config(state='disabled')
                 self.input_entry.config(state=tk.DISABLED) # Ensure input disabled
                 self.send_button.config(state=tk.DISABLED)
                 self._update_status_bar()
             return

         try:
             index = selection[0]
             list_text_with_marker = self.my_channels_listbox.get(index)
             # Remove potential marker before extracting name
             list_text = list_text_with_marker.lstrip("* ")
             new_channel_name = list_text.split(" (")[0] # Extract channel name

             if not new_channel_name: # Safety check if extraction failed
                 print("[ERROR] Failed to extract channel name from list selection.")
                 return

             # *** Check and clear notification state for the selected channel ***
             notification_cleared = False
             if new_channel_name in self.channel_notification_state:
                 # Use pop to remove the key and check if it existed (True if removed, None otherwise)
                 if self.channel_notification_state.pop(new_channel_name, None):
                     notification_cleared = True
                     print(f"[DEBUG] Cleared notification for {new_channel_name}")

             # Determine if the view needs updating
             # Update if: channel changed OR notification was just cleared (to redraw list or load chat)
             update_needed = (new_channel_name != self.current_channel) or notification_cleared

             if update_needed:
                 # Check if the channel *actually* changed (for reloading chat content)
                 channel_actually_changed = (new_channel_name != self.current_channel)

                 # Update the current channel tracker
                 self.current_channel = new_channel_name

                 # --- Reload chat content ONLY if the channel actually changed ---
                 if channel_actually_changed:
                    print(f"Switched to channel: {self.current_channel}") # Console log
                    # Clear the chat area
                    self.chat_area.config(state='normal')
                    self.chat_area.delete('1.0', tk.END)
                    # Load messages from the buffer for this channel
                    messages_to_display = self.channel_display_buffers.get(self.current_channel, [])
                    if not messages_to_display:
                        self.chat_area.insert(tk.END, f"Message history for '{self.current_channel}' (This session).\n", "SYSTEM")
                    else:
                        for msg in messages_to_display:
                            tag = "CHANNEL_MSG"
                            # Detect self message
                            if f"] {MY_NAME} " in msg and (msg.strip().endswith(":") or f"] {MY_NAME} (You):" in msg or f"] {MY_NAME} (Offline Queued):" in msg) :
                                tag = "SELF_MSG"
                            # Ensure newline before inserting
                            if not msg.endswith('\n'): msg += '\n'
                            self.chat_area.insert(tk.END, msg, tag)
                    self.chat_area.config(state='disabled')
                    self.chat_area.see(tk.END) # Scroll to bottom after loading

                 # --- Update UI state (enable input, set focus) ---
                 can_type = initial_login_info is not None # Check if logged in
                 self.input_entry.config(state=tk.NORMAL if can_type else tk.DISABLED)
                 self.send_button.config(state=tk.NORMAL if can_type else tk.DISABLED)
                 if can_type: self.input_entry.focus_set() # Set focus if enabled

                 # --- Redraw channel list ONLY if notification was cleared ---
                 if notification_cleared:
                      print(f"[DEBUG] Redrawing channel list after clearing notification for {new_channel_name}")
                      # Get current channel data safely to pass to update function
                      with my_channels_lock:
                          current_my_channels = my_channels.copy()
                      self._update_my_channels_list(current_my_channels) # This redraws the list

                 # --- Update status bar ---
                 self._update_status_bar() # Update status bar reflecting the new current channel


             # Case: Re-selected the same channel, and no notification was cleared
             # (i.e., update_needed was False)
             elif new_channel_name == self.current_channel and not notification_cleared:
                 # Just ensure input state is correct and focus is set
                 # No need to reload chat or redraw list
                 can_type = initial_login_info is not None
                 self.input_entry.config(state=tk.NORMAL if can_type else tk.DISABLED)
                 self.send_button.config(state=tk.NORMAL if can_type else tk.DISABLED)
                 if can_type: self.input_entry.focus_set()
                 # No need to update status bar if channel didn't change

         except Exception as e:
             # Log error and reset state
             self.log_message(f"Error selecting channel: {e}", level="ERROR")
             print(f"CRITICAL Error during channel selection: {e}")
             import traceback
             traceback.print_exc()
             self.current_channel = None
             self.input_entry.config(state=tk.DISABLED)
             self.send_button.config(state=tk.DISABLED)
             self._update_status_bar()

    def _on_avail_channel_doubleclick(self, event):
        """Handles double-click on available channel to initiate join."""
        selection = self.avail_channels_listbox.curselection()
        if not selection: return
        index = selection[0]
        list_text = self.avail_channels_listbox.get(index)
        # Extract channel name
        channel_name = list_text.split(" (")[0]
        if channel_name:
            if messagebox.askyesno("Join Channel", f"Do you want to attempt to join channel '{channel_name}'?"):
                self.log_message(f"Attempting to join channel '{channel_name}'...")
                # Run join in background thread
                threading.Thread(target=join_channel, args=(channel_name,), daemon=True).start()


    def _send_message_or_command(self, event=None):
        """Sends input text as message to current channel or executes command."""
        input_text = self.input_var.get()
        if not input_text: return

        # Clear input field immediately
        self.input_var.set("")

        # --- Command Handling ---
        if input_text.startswith('/'):
            self.log_message(f"Executing command: {input_text}", level="INFO")
            parts = input_text.lower().split(' ', 1)
            command = parts[0]
            args = parts[1] if len(parts) > 1 else ""

            if command == '/list': self._list_peers()
            elif command == '/list_channels': self._list_channels()
            elif command == '/myinfo': self._show_myinfo()
            elif command == '/create': self._create_channel_dialog(args)
            elif command == '/join': self._join_channel_dialog(args)
            elif command == '/my_channels': self._show_my_channels()
            elif command == '/members': self._show_channel_members(args)
            elif command == '/history': self._show_channel_history(args)
            elif command == '/quit': self._on_closing()
            elif command == '/invisible': self._toggle_visibility()
            elif command == '/offline': self._go_offline_logic()
            elif command == '/online': self._go_online_logic()
            elif command == '/msg': self._handle_msg_command(args)
            elif command == '/clear': self.chat_area.config(state='normal'); self.chat_area.delete('1.0', tk.END); self.chat_area.config(state='disabled'); self.log_message("Chat log cleared.")
            elif command == '/help': self._show_help()

            else: self.log_message(f"Unknown command: {command}", level="ERROR")

        # --- Channel Message Sending ---
        elif self.current_channel and initial_login_info: # Check login status via initial_login_info
            print(f"[DEBUG] GUI initiating send to {self.current_channel} (Offline: {is_offline})") # Console Debug
            # Call send_channel_msg - it will handle online/offline logic internally
            # Run in a thread to avoid blocking GUI, especially for online sends
            threading.Thread(target=send_channel_msg,
                             args=(self.current_channel, input_text, registry_socket),
                             daemon=True).start()
        elif not self.current_channel:
            self.log_message("Cannot send message: No channel selected.", level="ERROR")
        elif not initial_login_info:
             # Should not happen if UI state is managed correctly
             self.log_message("Cannot send message: Not logged in.", level="ERROR")

    def _handle_msg_command(self, args):
        """Handles /msg <channel> <message> command"""
        parts = args.split(' ', 1)
        if len(parts) == 2:
            channel_name = parts[0].strip()
            message_content = parts[1] # Keep original case
            if channel_name and message_content:
                 if is_offline:
                     self.log_message(f"Queueing message for {channel_name} via /msg (offline)...")
                     send_channel_msg(channel_name, message_content, None)
                 else:
                     self.log_message(f"Sending message to {channel_name} via /msg...")
                     threading.Thread(target=send_channel_msg,
                                     args=(channel_name, message_content, registry_socket),
                                     daemon=True).start()
            else:
                self.log_message("Usage: /msg <channel_name> <message content>", level="ERROR")
        else:
             self.log_message("Usage: /msg <channel_name> <message content>", level="ERROR")


    def _list_peers(self):
        """Requests peer list from registry."""
        if is_offline or not registry_socket:
            self.log_message("Cannot list peers: Offline or not connected.", level="ERROR")
            return
        self.log_message("Requesting peer list...")
        try:
            msg = {'type': 'list'} # Add ID if server requires it
            registry_socket.sendall((json.dumps(msg) + "\n").encode('utf-8'))
        except Exception as e:
            self.log_message(f"Error sending list peers request: {e}", level="ERROR")


    def _list_channels(self):
        """Requests available channel list from registry."""
        if is_offline or not registry_socket:
            self.log_message("Cannot list channels: Offline or not connected.", level="ERROR")
            return
        self.log_message("Requesting channel list...")
        try:
            msg = {'type': 'request_channel_list'}
            registry_socket.sendall((json.dumps(msg) + "\n").encode('utf-8'))
        except Exception as e:
            self.log_message(f"Error sending list channels request: {e}", level="ERROR")

    def _create_channel_dialog(self, args=""):
        """Opens dialog to create a channel."""
        if is_offline:
             self.log_message("Cannot create channel: Offline.", level="ERROR")
             return

        prefill_name, prefill_type = "", ""
        if args:
             parts = args.split(' ', 1)
             prefill_name = parts[0]
             if len(parts) > 1: prefill_type = parts[1]


        channel_name = simpledialog.askstring("Create Channel", "Enter Channel Name:", initialvalue=prefill_name, parent=self)
        if not channel_name: return # User cancelled
        channel_name = channel_name.strip()

        channel_type = simpledialog.askstring("Create Channel", "Enter Channel Type (public/private):", initialvalue=prefill_type, parent=self)
        if not channel_type: return # User cancelled
        channel_type = channel_type.strip().lower()

        if channel_name and channel_type:
            if channel_type not in ['public', 'private']:
                 messagebox.showerror("Error", "Channel type must be 'public' or 'private'.")
                 return
            if ' ' in channel_name:
                 messagebox.showerror("Error", "Channel name cannot contain spaces.")
                 return

            self.log_message(f"Attempting to create channel '{channel_name}' ({channel_type})...")
            # Run create in thread
            threading.Thread(target=create_channel,
                             args=(registry_socket, channel_name, channel_type),
                             daemon=True).start()
        else:
            messagebox.showerror("Error", "Channel name and type are required.")


    def _join_channel_dialog(self, args=""):
        """Opens dialog to join a channel."""
        if is_offline:
             self.log_message("Cannot join channel: Offline.", level="ERROR")
             return

        channel_name = simpledialog.askstring("Join Channel", "Enter Channel Name to Join:", initialvalue=args.strip(), parent=self)
        if not channel_name: return # User cancelled
        channel_name = channel_name.strip()

        if channel_name:
            self.log_message(f"Attempting to join channel '{channel_name}'...")
            # Run join in thread
            threading.Thread(target=join_channel, args=(channel_name,), daemon=True).start()


    def _toggle_visibility(self):
        """Toggles invisible state and notifies server if online."""
        global is_invisible
        is_invisible = not is_invisible
        status = "invisible" if is_invisible else "visible"
        self.log_message(f"Visibility set to {status}.")
        self.visibility_button.config(text="Go Visible" if is_invisible else "Go Invisible")

        if not is_offline and registry_socket:
            self.log_message("Updating visibility status with server...")
            try:
                msg = {'type': 'update_visibility', 'is_invisible': is_invisible}
                registry_socket.sendall((json.dumps(msg) + "\n").encode('utf-8'))
            except Exception as e:
                self.log_message(f"Error updating visibility with server: {e}", level="ERROR")
                # Revert state maybe? Or just log error.
                # is_invisible = not is_invisible # Revert on error
                # self.log_message(f"Visibility update failed, reverted to { 'invisible' if is_invisible else 'visible' }.", level="WARN")
        elif is_offline:
             self.log_message("Visibility change will apply when next online.")

        self._update_status_bar()


    def _toggle_online_offline(self):
        """Handles button click to go online or offline."""
        if is_offline:
            self._go_online_logic()
        else:
            self._go_offline_logic()

    def _go_offline_logic(self, notify_server=True):
        """Handles the logic to go offline."""
        global is_offline, registry_socket
        if is_offline:
            self.log_message("Already offline.", level="INFO")
            return

        self.log_message("Going offline...")
        # 1. Notify server (optional)
        if notify_server and registry_socket:
            try:
                self.log_message("Notifying server of offline status...")
                msg = {'type': 'update_offline_status', 'is_offline': True}
                registry_socket.sendall((json.dumps(msg) + "\n").encode('utf-8'))
            except Exception as e:
                self.log_message(f"Warning: Failed to send offline status notification: {e}", level="WARN")

        # 2. Stop listeners
        self._stop_listeners()

        # 3. Close registry socket
        if registry_socket:
            self.log_message("Closing registry connection...")
            try:
                registry_socket.close()
            except Exception as e:
                 self.log_message(f"Warning: Error closing registry socket: {e}", level="WARN")
            registry_socket = None

        # 4. Update state
        is_offline = True
        self.log_message("You are now offline. Network listeners stopped.")
        self._update_status_bar()
        self._set_ui_state(logged_in=True, online=False)


    def _go_online_logic(self):
        """Handles the logic to go online (reconnect)."""
        global is_offline, registry_socket
        if not is_offline:
            self.log_message("Already online.", level="INFO")
            return

        self.log_message("Attempting to go online (reconnect)...")
        self._set_ui_state(logged_in=True, online=False) # Keep UI mostly disabled during attempt
        self.online_offline_button.config(state=tk.DISABLED) # Disable button during attempt

        # Run reconnect in background thread
        threading.Thread(target=self._execute_reconnect, daemon=True).start()


    def _execute_reconnect(self):
         """Called in thread to perform reconnection."""
         global is_offline, registry_socket
         success, message, sock = attempt_reconnect_and_register(REGISTRY_HOST, REGISTRY_PORT)

         # Send result back to GUI thread
         gui_queue.put(("RECONNECT_RESULT", success, message))

         if success:
             is_offline = False
             # Send online status update
             if sock:
                 try:
                     online_msg = {'type': 'update_offline_status', 'is_offline': False}
                     sock.sendall((json.dumps(online_msg) + "\n").encode('utf-8'))
                 except Exception as e:
                     gui_queue.put(f"[WARN] Failed to send online status update after reconnect: {e}")

             # Restart listeners with the new socket
             self._start_network_listeners(sock)

             # Sync offline messages and potentially owner logs
             # Add a small delay to allow listeners to fully start?
             time.sleep(0.5)
             gui_queue.put("[INFO] Synchronizing data after reconnect...")
             if sock:
                 # Run syncs in background thread to avoid blocking listener start
                 threading.Thread(target=synchronize_host_server, args=(sock,), daemon=True).start()
                 time.sleep(0.1) # Tiny delay between syncs
                 threading.Thread(target=synchronize_offline_messages, args=(sock,), daemon=True).start()
         else:
             # Reconnect failed, remain offline
             is_offline = True
             registry_socket = None
             # Re-enable the 'Go Online' button via the queue handler (_handle_reconnect_result)



    def _show_myinfo(self):
        """Displays user's info."""
        info_str = f"Name: {MY_NAME}\n"
        info_str += f"Status: {'Guest' if is_guest else f'User (ID: {MY_ID})'}\n"
        info_str += f"State: {'OFFLINE' if is_offline else 'ONLINE'}\n"
        if not is_offline:
            info_str += f"Visibility: {'Invisible' if is_invisible else 'Visible'}\n"
        info_str += f"P2P Address: {MY_INFO.get('ip', '?')}:{MY_INFO.get('p2p_port', '?')}"
        messagebox.showinfo("My Info", info_str, parent=self)

    def _show_my_channels(self):
        """Displays channels the user is currently in."""
        if not my_channels:
             messagebox.showinfo("My Channels", "You are not currently in any channels.", parent=self)
             return

        info_str = "Channels you are in:\n---------------------\n"
        with my_channels_lock:
            sorted_channels = sorted(my_channels.items())
            for name, data in sorted_channels:
                role = "Owner" if data.get('owner') == MY_NAME else "Member"
                owner_display = f"(Owner: {data.get('owner', '?')})"
                member_count = len(data.get('members', {}))
                count_display = f", {member_count} members"
                info_str += f"- {name} ({role}{count_display if role=='Owner' else ''}) {owner_display if role=='Member' else ''}\n"

        # Display in a popup or log area? Popup for now.
        # Consider a dedicated window if list gets long.
        messagebox.showinfo("My Channels", info_str, parent=self)


    def _show_channel_members(self, channel_name_arg):
        """Shows members of a specified channel."""
        channel_name = channel_name_arg.strip()
        if not channel_name:
             # Try getting from selection if no arg provided
             selection = self.my_channels_listbox.curselection()
             if selection:
                  list_text = self.my_channels_listbox.get(selection[0])
                  channel_name = list_text.split(" (")[0]
             else:
                  self.log_message("Usage: /members <channel_name> (or select from 'My Channels')", level="ERROR")
                  return

        with my_channels_lock:
            channel_data = my_channels.get(channel_name)

        if not channel_data:
            self.log_message(f"You are not currently in channel '{channel_name}'.", level="ERROR")
            return

        members_dict = channel_data.get('members', {})
        owner = channel_data.get('owner')
        member_names = sorted(members_dict.keys())

        info_str = f"Members of '{channel_name}':\n---------------------\n"
        if not member_names:
            info_str += "(No members found locally)" # Should at least have self/owner
        else:
            for name in member_names:
                flags = []
                if name == owner: flags.append("Owner")
                if name == MY_NAME: flags.append("You")
                # Add online/offline status from known_peers? More complex lookup needed
                # member_info = members_dict.get(name, {})
                # peer_status = self._get_peer_registry_status(name) # Helper needed
                # if peer_status: flags.append(peer_status)

                flags_str = f" ({', '.join(flags)})" if flags else ""
                info_str += f"- {name}{flags_str}\n"

        messagebox.showinfo(f"Members - {channel_name}", info_str, parent=self)


    def _show_channel_history(self, channel_name_arg):
         """Shows locally stored history for an owned channel."""
         channel_name = channel_name_arg.strip()
         if not channel_name:
             # Try getting from selection if no arg provided
             selection = self.my_channels_listbox.curselection()
             if selection:
                  list_text = self.my_channels_listbox.get(selection[0])
                  channel_name = list_text.split(" (")[0]
             else:
                  self.log_message("Usage: /history <channel_name> (requires ownership, or select from 'My Channels')", level="ERROR")
                  return

         is_owner = False
         with my_channels_lock:
             is_owner = channel_name in my_channels and my_channels[channel_name].get('owner') == MY_NAME

         if not is_owner:
             self.log_message(f"Cannot show local history for '{channel_name}': You are not the owner.", level="ERROR")
             # Consider requesting history from owner/server if not owner? (Advanced)
             return

         log_entries = []
         with channel_logs_lock:
             # Get a copy of the log entries
             log_entries = channel_message_logs.get(channel_name, []).copy()

         history_str = f"Local History for '{channel_name}' (Owner View):\n"
         history_str += "--------------------------------------------\n"

         if not log_entries:
             history_str += "(No messages logged locally for this channel)"
         else:
             # Ensure sorted by timestamp
             try:
                  log_entries.sort(key=lambda x: datetime.datetime.fromisoformat(x['timestamp'].replace('Z', '+00:00')))
             except: pass # Ignore sort errors

             for entry in log_entries:
                 ts_str = entry.get('timestamp', '?')
                 sender = entry.get('sender', '?')
                 content = entry.get('content', '')
                 # Format timestamp nicely
                 try:
                     ts_obj = datetime.datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                     local_ts = ts_obj.astimezone()
                     time_display = local_ts.strftime('%Y-%m-%d %H:%M:%S')
                 except (ValueError, TypeError):
                     time_display = ts_str # Fallback to ISO string

                 history_str += f"[{time_display}] {sender}: {content}\n"

         # Display in a new window because it can be long
         history_window = tk.Toplevel(self)
         history_window.title(f"History - {channel_name}")
         history_window.geometry("500x400")
         text_area = scrolledtext.ScrolledText(history_window, wrap=tk.WORD, state='normal')
         text_area.pack(expand=True, fill=tk.BOTH, padx=5, pady=5)
         text_area.insert('1.0', history_str)
         text_area.config(state='disabled')
         close_button = tk.Button(history_window, text="Close", command=history_window.destroy)
         close_button.pack(pady=5)
         history_window.transient(self)
         history_window.grab_set()
         history_window.focus_set()


    def _show_help(self):
         """Displays available commands."""
         help_text = """Available Commands:
 /list                 - Request list of online peers from registry.
 /list_channels        - Request list of available channels from registry.
 /create <name> <type> - Create a channel (type: public/private).
 /join <name>          - Join an existing channel.
 /msg <channel> <msg>  - Send a message to a specific channel.
 /my_channels          - Show channels you are currently in.
 /members <channel>    - Show members of a channel you are in.
 /history <channel>    - Show local message history for a channel YOU OWN.
 /myinfo               - Display your current user information.
 /invisible            - Toggle your visibility status (hides from /list).
 /offline              - Disconnect from the registry and stop listeners.
 /online               - Attempt to reconnect to the registry.
 /clear                - Clear the chat display area.
 /quit                 - Exit the application.
 /help                 - Show this help message.

 Note: Selecting a channel from 'My Channels' list sets it for sending messages without /msg.
 Double-clicking a channel in 'Available Channels' initiates a join attempt.
 """
         messagebox.showinfo("Help - Commands", help_text, parent=self)


    def _on_closing(self):
        """Handles window close event."""
        if messagebox.askokcancel("Quit", "Are you sure you want to quit?"):
            self._quit_app()

    def _quit_app(self):
         """Performs cleanup and exits the application."""
         global running, registry_socket
         self.log_message("Quit requested. Shutting down...")
         running = False # Signal background threads

         # 1. Notify server if online
         if not is_offline and registry_socket:
             try:
                 # Send explicit offline status or just close? Explicit is better.
                 msg = {'type': 'update_offline_status', 'is_offline': True}
                 # Set short timeout for this final send?
                 registry_socket.settimeout(1.0)
                 registry_socket.sendall((json.dumps(msg) + "\n").encode('utf-8'))
                 self.log_message("Offline notification sent to server.")
             except Exception as e:
                  self.log_message(f"Warning: Failed to send final offline status: {e}", level="WARN")

         # 2. Stop listeners
         self._stop_listeners()

         # 3. Close socket if still open
         if registry_socket:
             try:
                 registry_socket.close()
             except Exception: pass # Ignore errors
             registry_socket = None

         self.log_message("Waiting briefly for threads...")
         time.sleep(0.3) # Short wait

         # 4. Destroy GUI window
         self.destroy()
         print("[CLOSED] Client shut down complete.") # Final console message


# --- Main Execution ---
if __name__ == "__main__":
    # Optional: Basic console logging before GUI starts
    print(f"Starting P2P Client GUI... (Will listen on port {MY_P2P_PORT})")
    print(f"Attempting to connect to registry at {REGISTRY_HOST}:{REGISTRY_PORT}")

    app = P2PClientGUI()
    app.mainloop()