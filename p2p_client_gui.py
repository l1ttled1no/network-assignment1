import tkinter as tk
from tkinter import scrolledtext, simpledialog, messagebox, Listbox, Frame, Label, Entry, Button, END, DISABLED, NORMAL
import socket
import threading
import sys
import time
import json
import random
import ipaddress
from queue import Queue, Empty # For thread-safe communication

# --- Configuration (Same as your original client) ---
REGISTRY_HOST = '127.0.0.1'
REGISTRY_PORT = 9999
LISTEN_HOST = '0.0.0.0' # Host for P2P listener
# MY_P2P_PORT will be set after getting username
BUFFER_SIZE = 4096

class P2PClientGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("P2P Chat Client")
        self.root.geometry("750x550") # Adjusted size

        # --- Core Client State (from your original code) ---
        self.peer_list_lock = threading.Lock()
        self.known_peers = {} # { peer_id: {'ip': ip, 'p2p_port': port, 'name': name} }
        self.my_channels_lock = threading.Lock()
        self.my_channels = {} # { channel_name: {'owner': owner_name, 'members': {name: info}} }
        self.running = True
        self.MY_NAME = None
        self.MY_INFO = {}
        self.MY_P2P_PORT = random.randint(10000, 60000) # Assign random P2P port
        self.registry_socket = None
        self.network_threads = []
        self.message_queue = Queue() # Queue for thread-GUI communication

        # --- Get Username ---
        self.get_username()
        if not self.MY_NAME:
             root.quit() # Exit if no username provided
             return

        self.root.title(f"P2P Chat Client - {self.MY_NAME}")

        # --- Build GUI ---
        self.create_widgets()

        # --- Start Networking ---
        if not self.start_networking():
             messagebox.showerror("Network Error", "Failed to start networking components. Exiting.")
             self.root.quit()
             return

        # --- Start Queue Processor ---
        self.process_queue()

        # --- Handle Window Closing ---
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def get_username(self):
        while not self.MY_NAME:
            name = simpledialog.askstring("Username", "Enter your desired name:", parent=self.root)
            if name is None: # User cancelled
                break
            name = name.strip()
            # Basic validation (add more rules if needed)
            if name and ' ' not in name and len(name) <= 50:
                self.MY_NAME = name
            elif name:
                messagebox.showwarning("Invalid Name", "Name cannot contain spaces and must be 50 chars or less.", parent=self.root)
            else:
                 messagebox.showwarning("Invalid Name", "Name cannot be empty.", parent=self.root)

    def create_widgets(self):
        # Main Frame
        main_frame = Frame(self.root, padx=5, pady=5)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Configure grid layout weights
        main_frame.columnconfigure(0, weight=3) # Message area wider
        main_frame.columnconfigure(1, weight=1) # Peer list narrower
        main_frame.rowconfigure(0, weight=1)    # Message/Peer list area expand vertically
        main_frame.rowconfigure(1, weight=0)    # Input area fixed height
        main_frame.rowconfigure(2, weight=0)    # Status bar fixed height

        # --- Message Display Area ---
        self.message_area = scrolledtext.ScrolledText(main_frame, wrap=tk.WORD, state=DISABLED, height=15)
        self.message_area.grid(row=0, column=0, padx=(0, 5), pady=(0, 5), sticky="nsew")

        # --- Peer List Area ---
        peer_frame = Frame(main_frame)
        peer_frame.grid(row=0, column=1, padx=(5, 0), pady=(0, 5), sticky="nsew")
        peer_frame.rowconfigure(1, weight=1)
        peer_frame.columnconfigure(0, weight=1)

        Label(peer_frame, text="Online Peers:").grid(row=0, column=0, sticky="w")
        self.peer_listbox = Listbox(peer_frame, height=10)
        self.peer_listbox.grid(row=1, column=0, sticky="nsew")
        # Add double-click functionality to peer list? (e.g., pre-fill /msg)
        # self.peer_listbox.bind("<Double-Button-1>", self.on_peer_double_click)

        # --- Input Area ---
        input_frame = Frame(main_frame)
        input_frame.grid(row=1, column=0, columnspan=2, pady=(5, 5), sticky="ew")
        input_frame.columnconfigure(0, weight=1)

        self.input_entry = Entry(input_frame, width=60)
        self.input_entry.grid(row=0, column=0, padx=(0, 5), sticky="ew")
        self.input_entry.bind("<Return>", self.send_input) # Bind Enter key

        self.send_button = Button(input_frame, text="Send", command=self.send_input)
        self.send_button.grid(row=0, column=1, sticky="e")

        # --- Status Bar ---
        self.status_var = tk.StringVar()
        self.status_var.set(f"P2P Port: {self.MY_P2P_PORT} | Status: Initializing...")
        status_bar = Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.grid(row=2, column=0, columnspan=2, sticky="ew")

    # --- Core Logic Integration ---

    def start_networking(self):
        """Starts P2P listener and connects to Registry."""
        self.log_message(f"[INFO] Starting P2P listener on {LISTEN_HOST}:{self.MY_P2P_PORT}...")
        try:
            # Start P2P listener thread
            p2p_thread = threading.Thread(target=self.listen_for_peers, args=(LISTEN_HOST, self.MY_P2P_PORT), daemon=True)
            p2p_thread.start()
            self.network_threads.append(p2p_thread)
            time.sleep(0.1) # Give listener a moment
        except Exception as e:
            self.log_message(f"[CRITICAL] Failed to start P2P listener: {e}")
            return False

        # Connect to Registry
        self.log_message(f"[INFO] Connecting to Registry {REGISTRY_HOST}:{REGISTRY_PORT}...")
        try:
            self.registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.registry_socket.settimeout(10.0) # Connection timeout
            self.registry_socket.connect((REGISTRY_HOST, REGISTRY_PORT))
            self.registry_socket.settimeout(None) # Reset timeout after connection
            self.log_message("[INFO] Connected to Registry.")
            self.update_status("Connected to Registry")

            # Start Registry listener thread
            registry_thread = threading.Thread(target=self.listen_to_registry, args=(self.registry_socket,), daemon=True)
            registry_thread.start()
            self.network_threads.append(registry_thread)
            return True

        except socket.timeout:
            self.log_message(f"[ERROR] Timeout connecting to Registry server at {REGISTRY_HOST}:{REGISTRY_PORT}.")
            self.update_status("Error: Connection Timeout")
            return False
        except socket.error as e:
            self.log_message(f"[ERROR] Could not connect to Registry server: {e}")
            self.update_status(f"Error: {e}")
            return False
        except Exception as e:
            self.log_message(f"[CRITICAL] Unexpected error connecting to Registry: {e}")
            self.update_status("Error: Connection Failed")
            return False

    def on_closing(self):
        """Handles window close event."""
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            self.log_message("[INFO] Shutdown requested.")
            self.running = False # Signal threads to stop

            # Close registry connection
            if self.registry_socket:
                self.log_message("[INFO] Closing connection to registry...")
                try:
                    # Optional: send disconnect message
                    # self.registry_socket.sendall(json.dumps({'type':'disconnect'}).encode('utf-8')+b'\n')
                    self.registry_socket.shutdown(socket.SHUT_RDWR)
                except (socket.error, OSError): pass # Ignore errors if already closed
                try:
                    self.registry_socket.close()
                except (socket.error, OSError): pass

            # P2P listener socket is closed within its own thread's finally block when running=False

            # Wait briefly for threads (optional, daemons should exit anyway)
            # time.sleep(0.5)

            self.root.destroy() # Close the Tkinter window

    # --- Queue Handling ---

    def queue_message(self, msg_type, content):
        """Puts a message onto the queue for the GUI thread."""
        self.message_queue.put({'type': msg_type, 'content': content})

    def process_queue(self):
        """Periodically checks the queue and updates the GUI."""
        try:
            while True: # Process all messages currently in queue
                msg = self.message_queue.get_nowait()
                msg_type = msg.get('type')
                content = msg.get('content')

                if msg_type == 'log_message':
                    self.log_message(content, from_queue=True)
                elif msg_type == 'update_peer_list':
                    self.update_peer_listbox(content)
                elif msg_type == 'update_status':
                    self.update_status(content)
                elif msg_type == 'display_p2p_message':
                     # Content expected: {'sender': name, 'message': text}
                     sender = content.get('sender', 'Unknown Peer')
                     message_text = content.get('message', '')
                     self.log_message(f"[P2P from {sender}]: {message_text}", from_queue=True)
                elif msg_type == 'show_error':
                     messagebox.showerror("Error", content, parent=self.root)
                elif msg_type == 'show_info':
                     messagebox.showinfo("Info", content, parent=self.root)
                 # Add more message types as needed (e.g., update_channel_list)

        except Empty:
            pass # Queue is empty, do nothing
        except Exception as e:
             # Log error processing queue item, but don't crash UI
             print(f"[GUI ERROR] Error processing queue: {e}")
             self.log_message(f"[GUI ERROR] Error processing queue: {e}", from_queue=True)


        # Reschedule after a short delay
        if self.running:
            self.root.after(100, self.process_queue) # Check queue every 100ms

    # --- GUI Update Methods (Called ONLY from GUI Thread via process_queue) ---

    def log_message(self, message, from_queue=False):
        """Appends a message to the main text area. If called from background thread, use queue."""
        if not from_queue and threading.current_thread() != threading.main_thread():
            self.queue_message('log_message', message)
            return

        # Ensure updates happen in the main thread
        if threading.current_thread() != threading.main_thread():
             # This should ideally not be reached if queueing works correctly
             print(f"WARNING: log_message called directly from thread {threading.current_thread().name}")
             self.queue_message('log_message', message) # Queue it anyway
             return

        try:
            self.message_area.config(state=NORMAL)
            timestamp = time.strftime("%H:%M:%S", time.localtime())
            self.message_area.insert(END, f"[{timestamp}] {message}\n")
            self.message_area.config(state=DISABLED)
            self.message_area.see(END) # Auto-scroll
        except tk.TclError as e:
             print(f"[GUI ERROR] Failed to update message area (window closed?): {e}")
        except Exception as e:
             print(f"[GUI ERROR] Unexpected error updating message area: {e}")

    def update_peer_listbox(self, peers_dict):
        """Updates the peer listbox display."""
        if threading.current_thread() != threading.main_thread():
            self.queue_message('update_peer_list', peers_dict)
            return

        try:
            self.peer_listbox.delete(0, END) # Clear existing list
            if peers_dict:
                # Sort by name
                sorted_peers = sorted(peers_dict.values(), key=lambda x: x.get('name', 'zzzz'))
                for info in sorted_peers:
                    name = info.get('name', 'Unknown')
                    # Optionally add IP:Port info
                    # display_text = f"{name} ({info.get('ip', 'N/A')}:{info.get('p2p_port', 'N/A')})"
                    if name != self.MY_NAME: # Don't list self
                        self.peer_listbox.insert(END, name)
        except tk.TclError as e:
             print(f"[GUI ERROR] Failed to update peer listbox (window closed?): {e}")
        except Exception as e:
             print(f"[GUI ERROR] Unexpected error updating peer listbox: {e}")


    def update_status(self, message):
        """Updates the status bar text."""
        if threading.current_thread() != threading.main_thread():
            self.queue_message('update_status', message)
            return
        try:
             self.status_var.set(f"P2P Port: {self.MY_P2P_PORT} | Status: {message}")
        except tk.TclError as e:
             print(f"[GUI ERROR] Failed to update status bar (window closed?): {e}")
        except Exception as e:
             print(f"[GUI ERROR] Unexpected error updating status bar: {e}")

    # --- Input Handling ---

    def send_input(self, event=None): # event=None allows binding to Enter key
        """Handles text entered in the input box."""
        command = self.input_entry.get().strip()
        if not command:
            return

        self.log_message(f"> {command}") # Log the command locally
        self.input_entry.delete(0, END) # Clear input field

        # Parse and execute command
        try:
            if command.lower() == '/quit':
                self.on_closing()
            elif command.lower() == '/list':
                # Display known peers from local state
                self.log_message("[KNOWN PEERS]:")
                with self.peer_list_lock:
                     if self.known_peers:
                         sorted_peers = sorted(self.known_peers.values(), key=lambda x: x.get('name', 'zzzz'))
                         count = 0
                         for info in sorted_peers:
                             if info.get('name') != self.MY_NAME:
                                 self.log_message(f"  - {info.get('name', 'Unknown')}")
                                 count += 1
                         if count == 0: self.log_message("  (No other peers online)")
                     else: self.log_message("  (List not received or empty)")
            elif command.lower() == '/myinfo':
                 self.log_message("[YOUR INFO]:")
                 if self.MY_INFO:
                     self.log_message(f"  Name: {self.MY_INFO.get('name')}")
                     self.log_message(f"  P2P Address: {self.MY_INFO.get('ip')}:{self.MY_INFO.get('p2p_port')}")
                 else: self.log_message("  (Own info not yet confirmed by registry)")

            elif command.lower().startswith('/msg '):
                parts = command.split(' ', 2)
                if len(parts) == 3:
                    peer_name, message = parts[1], parts[2]
                    if peer_name == self.MY_NAME: self.log_message("[CMD ERROR] Cannot send message to yourself.")
                    else:
                         # Run send in a separate thread to avoid blocking GUI if peer is slow/unreachable
                         send_thread = threading.Thread(target=self.send_p2p_message_thread, args=(peer_name, message), daemon=True)
                         send_thread.start()
                else: self.log_message("[CMD ERROR] Usage: /msg <name> <message>")

            elif command.lower().startswith('/create '):
                parts = command.split(' ', 1)
                if len(parts) == 2:
                    channel_name = parts[1].strip()
                    if channel_name:
                         # Needs registry socket - make sure it's available
                         if self.registry_socket:
                              # Run in thread? Creation is usually fast, but socket errors can block
                              # For simplicity, call directly for now, but thread is safer
                              if not self.create_channel(self.registry_socket, channel_name):
                                   self.log_message("[CMD ERROR] Failed to send create channel request.")
                         else:
                              self.log_message("[CMD ERROR] Not connected to registry.")
                    else: self.log_message("[CMD ERROR] Channel name cannot be empty.")
                else: self.log_message("[CMD ERROR] Usage: /create <channel_name>")

            elif command.lower() == '/ch_list':
                 self.log_message("[YOUR CHANNELS]: (Channels you own or joined)")
                 with self.my_channels_lock:
                     if self.my_channels:
                         sorted_channels = sorted(self.my_channels.items())
                         for name, data in sorted_channels:
                             role = "Owner" if data.get('owner') == self.MY_NAME else "Member"
                             owner_name = data.get('owner', 'N/A')
                             count_str = f"{len(data.get('members',{}))} members" if role == "Owner" else "? members"
                             self.log_message(f"  - {name} ({role}, Owner: {owner_name}, {count_str})")
                     else: self.log_message("  (You are not in any channels yet)")

            elif command.lower().startswith('/ch_members '):
                parts = command.split(' ', 1)
                if len(parts) == 2:
                    channel_name = parts[1].strip()
                    if channel_name:
                        with self.my_channels_lock:
                            if channel_name in self.my_channels:
                                data = self.my_channels[channel_name]
                                if data.get('owner') == self.MY_NAME:
                                    self.log_message(f"[MEMBERS of '{channel_name}']: (Owner)")
                                    members = data.get('members', {})
                                    if members:
                                        for name in sorted(members.keys()):
                                            self.log_message(f"  - {name} {'(You)' if name==self.MY_NAME else ''}")
                                    else: self.log_message("  (Error: Member list empty?)")
                                else:
                                    self.log_message(f"[INFO] You are not the owner of '{channel_name}'. Owner ({data.get('owner','?')}) has the list.")
                            else: self.log_message(f"[CMD ERROR] You are not part of channel '{channel_name}'.")
                    else: self.log_message("[CMD ERROR] Channel name cannot be empty.")
                else: self.log_message("[CMD ERROR] Usage: /ch_members <channel_name>")

            elif command.lower() == '/list_channels':
                 self.log_message("[CMD] Requesting list of all channels...")
                 if self.registry_socket:
                      try:
                          req_msg = json.dumps({'type': 'request_channel_list'}) + "\n"
                          self.registry_socket.sendall(req_msg.encode('utf-8'))
                      except (socket.error, OSError) as e:
                          self.log_message(f"[ERROR] Failed sending channel list request: {e}")
                 else: self.log_message("[CMD ERROR] Not connected to registry.")

            else:
                self.log_message(f"[CMD ERROR] Unknown command: {command.split(' ')[0]}")
                self.log_message("  Available: /list, /myinfo, /msg, /create, /ch_list, /ch_members, /list_channels, /quit")

        except Exception as e:
             self.log_message(f"[CMD ERROR] Error processing command: {e}")
             import traceback
             traceback.print_exc() # For debugging


    # --- Networking Methods (Run in Threads) ---

    # Adapt methods from your original client code to use self. variables
    # and self.log_message() or self.queue_message() instead of print()

    # --- Helper Functions (Adapted for Class) ---
    def get_local_ip(self):
        # (Same logic as your original get_local_ip)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(0.1)
            s.connect(('8.8.8.8', 80)); ip = s.getsockname()[0]; s.close(); return ip
        except:
            try: return socket.gethostbyname(socket.gethostname())
            except: return '127.0.0.1'

    def update_my_info(self, name, p2p_port, ip=None):
        if ip is None:
            determined_ip = self.get_local_ip()
            if LISTEN_HOST == '0.0.0.0' and ipaddress.ip_address(determined_ip).is_loopback:
                self.log_message(f"[WARNING] Determined local IP is loopback ({determined_ip}). P2P might only work locally.")
            self.MY_INFO = {'ip': determined_ip, 'p2p_port': p2p_port, 'name': name}
            self.log_message(f"[INFO] Updated own info (determined): {self.MY_INFO}")
        else:
            self.MY_INFO = {'ip': ip, 'p2p_port': p2p_port, 'name': name}
            self.log_message(f"[INFO] Updated own info (from server): {self.MY_INFO}")

    def get_peer_info_by_name(self, name):
        if name == self.MY_NAME: return self.MY_INFO
        with self.peer_list_lock:
            for info in self.known_peers.values(): # Iterate values directly
                if info.get('name') == name:
                    return info
        return None

    def get_peer_name_by_address(self, ip, port):
        # Note: This might be less reliable if peer connects from ephemeral port
        if self.MY_INFO.get('ip') == ip and self.MY_INFO.get('p2p_port') == port:
            return self.MY_NAME + " (Self)"
        with self.peer_list_lock:
            for info in self.known_peers.values():
                # Check peer's *listening* address info
                if info.get('ip') == ip and info.get('p2p_port') == port:
                    return info.get('name', f"{ip}:{port}")
        return f"{ip}:{port}"

    # --- Create Channel (Needs registry socket) ---
    def create_channel(self, registry_socket, channel_name):
        if not self.MY_NAME or not self.MY_INFO:
            self.log_message("[ERROR] Cannot create channel: Client name or info missing.")
            return False
        try:
            msg_dict = {'type': 'create_channel','channel_name': channel_name,'owner_name': self.MY_NAME,'owner_ip': self.MY_INFO.get('ip'),'owner_p2p_port': self.MY_INFO.get('p2p_port')}
            msg_json = json.dumps(msg_dict) + "\n"
            self.log_message(f"[CMD] Sending create request: {msg_json.strip()}")
            registry_socket.sendall(msg_json.encode('utf-8'))
            self.log_message(f"[CMD] Request sent for '{channel_name}'. Waiting for confirmation...")
            return True
        except (socket.error, OSError) as e:
            self.log_message(f"[ERROR] Socket error sending create channel request: {e}")
            return False
        except Exception as e:
            self.log_message(f"[ERROR] Unexpected error sending create channel request: {e}")
            return False

    # --- P2P Listener Thread ---
    def listen_for_peers(self, host, port):
        p2p_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        p2p_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            p2p_server_socket.bind((host, port))
            p2p_server_socket.listen(5)
            self.log_message(f"[P2P LISTENING] Ready on {host}:{port}")
        except OSError as e:
            self.log_message(f"[P2P BIND ERROR] {e}. Port {port} busy?")
            self.queue_message('show_error', f"P2P Listen Error: {e}. Port {port} may be in use. Try restarting.")
            self.running = False # Stop client if P2P can't start
            return
        except Exception as e:
             self.log_message(f"[P2P LISTENER CRITICAL] {e}")
             self.running = False
             return

        while self.running:
            try:
                p2p_server_socket.settimeout(1.0)
                peer_socket, peer_address = p2p_server_socket.accept()
                p2p_server_socket.settimeout(None)
                # Handle connection in a new thread
                conn_thread = threading.Thread(target=self.handle_peer_connection, args=(peer_socket, peer_address), daemon=True)
                conn_thread.start()
                # self.network_threads.append(conn_thread) # Optionally track connection threads
            except socket.timeout: continue
            except socket.error as e:
                if self.running: self.log_message(f"[P2P LISTENER ERROR] {e}")
                break
            except Exception as e:
                 if self.running: self.log_message(f"[P2P LISTENER CRITICAL] {e}")
                 break
        self.log_message("[P2P LISTENING] Shutting down listener.")
        p2p_server_socket.close()

    def handle_peer_connection(self, peer_socket, peer_address):
        # Simplified - just logs messages for now. Needs parsing logic.
        peer_name = self.get_peer_name_by_address(peer_address[0], peer_address[1])
        self.log_message(f"[P2P <<] Connection from {peer_name} ({peer_address[0]}:{peer_address[1]})")
        try:
            while self.running:
                data = peer_socket.recv(BUFFER_SIZE)
                if not data:
                    self.log_message(f"[P2P <<] Peer {peer_name} disconnected.")
                    break
                try:
                     message_text = data.decode('utf-8')
                     # TODO: Parse different P2P message types here (join, channel msg, etc.)
                     # For now, just display raw message via queue
                     self.queue_message('display_p2p_message', {'sender': peer_name, 'message': message_text})
                except UnicodeDecodeError:
                     self.log_message(f"[P2P WARNING] Received non-UTF8 data from {peer_name}.")
                except Exception as e:
                     self.log_message(f"[P2P ERROR] Error decoding/processing message from {peer_name}: {e}")

        except ConnectionResetError: self.log_message(f"[P2P <<] Peer {peer_name} connection reset.")
        except (socket.error, OSError) as e:
            if self.running: self.log_message(f"[P2P ERROR] Socket error with {peer_name}: {e}")
        except Exception as e: self.log_message(f"[P2P ERROR] Unexpected error with peer {peer_name}: {e}")
        finally: peer_socket.close()


    # --- Registry Listener Thread ---
    def listen_to_registry(self, registry_socket):
        buffer = ""
        while self.running:
            try:
                data = registry_socket.recv(BUFFER_SIZE)
                if not data:
                    self.log_message("[CONNECTION LOST] Registry server closed connection.")
                    self.update_status("Disconnected from Registry")
                    self.running = False
                    break
                buffer += data.decode('utf-8')

                while '\n' in buffer:
                    message_json, buffer = buffer.split('\n', 1)
                    if not message_json: continue
                    try:
                        message = json.loads(message_json)
                        msg_type = message.get('type')
                        # self.log_message(f"[DEBUG REGISTRY] Received: {msg_type}") # Verbose Debug

                        if msg_type == 'request_p2p_port':
                            if self.MY_NAME:
                                self.update_my_info(self.MY_NAME, self.MY_P2P_PORT)
                                reg_msg = json.dumps({'type': 'register','p2p_port': self.MY_P2P_PORT,'name': self.MY_NAME}) + "\n"
                                registry_socket.sendall(reg_msg.encode('utf-8'))
                                self.log_message(f"[REGISTRY] Sent registration for '{self.MY_NAME}'.")
                            else: self.log_message("[ERROR] Cannot register, name not set.")

                        elif msg_type == 'registration_ack':
                            self.log_message(f"[REGISTRY] {message.get('message', 'Registration acknowledged.')}")
                            client_ip = message.get('client_ip')
                            if client_ip: self.update_my_info(self.MY_NAME, self.MY_P2P_PORT, ip=client_ip)
                            self.update_status("Registered") # Update status bar

                        elif msg_type == 'peer_list':
                            self.log_message("[REGISTRY] Received peer list.")
                            server_peers = message.get('peers', {})
                            with self.peer_list_lock: self.known_peers = server_peers
                            self.queue_message('update_peer_list', server_peers) # Update GUI listbox
                            # Update self info if necessary based on list
                            my_info_in_list = next((info for pid, info in server_peers.items() if info.get('name') == self.MY_NAME), None)
                            if my_info_in_list:
                                 if self.MY_INFO.get('ip') != my_info_in_list.get('ip'):
                                      self.update_my_info(self.MY_NAME, self.MY_P2P_PORT, ip=my_info_in_list.get('ip'))


                        elif msg_type == 'peer_joined':
                            p_info = message.get('peer_info')
                            p_name = p_info.get('name','?') if p_info else '?'
                            p_id = message.get('id','?')
                            if p_info and p_id and p_name != self.MY_NAME:
                                with self.peer_list_lock: self.known_peers[p_id] = p_info
                                self.log_message(f"[REGISTRY] Peer joined: {p_name}")
                                self.queue_message('update_peer_list', self.known_peers.copy()) # Send copy

                        elif msg_type == 'peer_left':
                            p_id = message.get('id')
                            if p_id:
                                removed_name = 'Unknown'
                                with self.peer_list_lock:
                                    if p_id in self.known_peers: removed_name = self.known_peers.pop(p_id).get('name', '?')
                                if removed_name != 'Unknown':
                                     self.log_message(f"[REGISTRY] Peer left: {removed_name}")
                                     self.queue_message('update_peer_list', self.known_peers.copy()) # Send copy

                        elif msg_type == 'channel_created':
                            c_name = message.get('channel_name')
                            c_owner = message.get('owner_name')
                            self.log_message(f"[REGISTRY] Channel '{c_name}' creation confirmed.")
                            if c_owner == self.MY_NAME:
                                with self.my_channels_lock:
                                    if c_name not in self.my_channels:
                                        self.log_message(f"[CHANNEL] Initializing local data for owned '{c_name}'.")
                                        my_members = {self.MY_NAME: self.MY_INFO.copy()} if self.MY_INFO else {}
                                        if not my_members: self.log_message("[WARNING] MY_INFO empty on channel create.")
                                        self.my_channels[c_name] = {'owner': self.MY_NAME,'members': my_members}

                        elif msg_type == 'channel_error':
                            err_msg = message.get('message', 'Unknown channel error')
                            c_name = message.get('channel_name', 'N/A')
                            self.log_message(f"[REGISTRY ERROR] Channel '{c_name}': {err_msg}")
                            self.queue_message('show_error', f"Channel Error ({c_name}): {err_msg}") # Show popup

                        elif msg_type == 'new_channel_available':
                            c_name = message.get('channel_name')
                            c_owner = message.get('owner_name')
                            self.log_message(f"[REGISTRY] New channel available: '{c_name}' (Owner: {c_owner})")

                        elif msg_type == 'channel_list':
                            self.log_message("[REGISTRY] Received list of all channels:")
                            channels = message.get('channels', {})
                            if channels:
                                for name in sorted(channels.keys()):
                                    info = channels[name]
                                    owner=info.get('owner_name','?')
                                    contact=f"{info.get('owner_ip','?')}:{info.get('owner_p2p_port','?')}"
                                    self.log_message(f"  - {name} (Owner: {owner} @ {contact})")
                            else: self.log_message("  (No channels registered)")

                        elif msg_type == 'error': # Generic error from server
                             err_msg = message.get('message', 'Unknown server error')
                             self.log_message(f"[REGISTRY ERROR] {err_msg}")
                             self.queue_message('show_error', f"Registry Error: {err_msg}")

                        elif msg_type == 'server_shutdown':
                            self.log_message(f"[REGISTRY] {message.get('message', 'Server is shutting down.')}")
                            self.update_status("Server Shutdown")
                            self.running = False # Trigger client shutdown

                        else:
                            self.log_message(f"[REGISTRY UNHANDLED] Type: {msg_type} | Data: {message_json}")

                    except json.JSONDecodeError: self.log_message(f"[REGISTRY ERROR] Invalid JSON: {message_json}")
                    except Exception as e: self.log_message(f"[ERROR] Processing registry msg: {e}\nMsg: {message_json}")

            except (ConnectionResetError, ConnectionAbortedError, socket.error, OSError) as e:
                if self.running:
                    self.log_message(f"[CONNECTION ERROR] Registry connection error: {e}")
                    self.update_status("Disconnected")
                    self.running = False
                break # Exit thread loop
            except UnicodeDecodeError:
                self.log_message("[ERROR] Received non-UTF8 data from registry.")
                buffer = "" # Attempt to clear bad data
            except Exception as e:
                if self.running:
                    self.log_message(f"[ERROR] Unexpected error receiving from registry: {e}")
                    self.update_status("Error")
                    self.running = False # Treat unexpected errors as fatal for registry connection
                break
        self.log_message("[THREAD] Registry listener finished.")
        # Ensure main loop stops if this thread stops unexpectedly
        if self.running:
             self.running = False
             # Maybe schedule a GUI popup about disconnection?
             self.queue_message('show_error', "Lost connection to the Registry Server.")


    # --- P2P Message Sending (Adapted for Class) ---
    def send_p2p_message_thread(self, target_name, message):
         """Wrapper to send P2P message in a background thread."""
         self.log_message(f"[P2P >>] Sending to {target_name}...")
         target_info = self.get_peer_info_by_name(target_name)
         if not target_info:
             self.log_message(f"[P2P ERROR] Peer '{target_name}' not found.")
             return
         target_ip = target_info.get('ip')
         target_port = target_info.get('p2p_port')
         if not target_ip or not target_port:
             self.log_message(f"[P2P ERROR] Incomplete info for '{target_name}'.")
             return

         try:
             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_socket:
                  peer_socket.settimeout(5.0)
                  peer_socket.connect((target_ip, target_port))
                  peer_socket.sendall(message.encode('utf-8'))
                  self.log_message(f"[P2P >>] Message sent successfully to {target_name}.")
         except socket.timeout: self.log_message(f"[P2P ERROR] Connection to {target_name} timed out.")
         except ConnectionRefusedError: self.log_message(f"[P2P ERROR] Connection to {target_name} refused.")
         except OSError as e: self.log_message(f"[P2P ERROR] Network error sending to {target_name}: {e}")
         except Exception as e: self.log_message(f"[P2P ERROR] Unexpected error sending to {target_name}: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    root = tk.Tk()
    app = P2PClientGUI(root)
    if app.MY_NAME: # Only run mainloop if username was provided
        root.mainloop()
    print("Client application finished.")