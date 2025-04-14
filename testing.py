import tkinter as tk
from tkinter import scrolledtext, messagebox, simpledialog
import threading
import queue
import sys
import json
import time

# --- Import backend logic ---
# Assuming your backend code is saved as p2p_client_backend.py
try:
    import p2p_client as backend
except ImportError:
    messagebox.showerror("Error", "Could not find p2p_client_backend.py.\nMake sure it's in the same directory.")
    sys.exit(1)

# --- GUI Queue ---
# Used to safely pass messages from backend threads to the GUI thread
gui_queue = queue.Queue()

# --- Text Redirector ---
# Redirects stdout/stderr to the GUI queue
class TextRedirector:
    def __init__(self, widget_queue, stream_name):
        self.widget_queue = widget_queue
        self.stream_name = stream_name # "stdout" or "stderr"

    def write(self, string):
        self.widget_queue.put(f"{string}") # Simple put, formatting handled later if needed

    def flush(self):
        pass # Tkinter's Text widget handles flushing implicitly

# --- Main Application Class ---
class P2PClientGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("P2P Chat Client")
        self.geometry("600x500")

        self.backend_thread = None
        self.message_poll_job = None
        self.is_logged_in = False

        # --- Redirect stdout/stderr ---
        # Do this early so even initial backend prints (like port binding) are caught
        sys.stdout = TextRedirector(gui_queue, "stdout")
        sys.stderr = TextRedirector(gui_queue, "stderr")
        print("[GUI] Standard output redirected.") # Test redirection

        self._create_login_widgets()
        self._create_main_widgets()

        # Start with login frame
        self.login_frame.pack(fill=tk.BOTH, expand=True)
        self.main_frame.pack_forget() # Hide main frame initially

        self.protocol("WM_DELETE_WINDOW", self._on_closing) # Handle window close

    def _create_login_widgets(self):
        self.login_frame = tk.Frame(self)

        tk.Label(self.login_frame, text="P2P Chat Login", font=("Helvetica", 16)).pack(pady=20)

        # Name
        tk.Label(self.login_frame, text="Name:").pack(pady=(10,0))
        self.name_entry = tk.Entry(self.login_frame, width=30)
        self.name_entry.pack()
        self.name_entry.focus_set() # Set focus here

        # ID (for registered users)
        tk.Label(self.login_frame, text="ID (Optional - for /login):").pack(pady=(10,0))
        self.id_entry = tk.Entry(self.login_frame, width=30)
        self.id_entry.pack()

        # Buttons
        button_frame = tk.Frame(self.login_frame)
        button_frame.pack(pady=20)

        self.guest_button = tk.Button(button_frame, text="Login as Guest", command=self._handle_guest_login)
        self.guest_button.pack(side=tk.LEFT, padx=10)

        self.login_button = tk.Button(button_frame, text="Login as User", command=self._handle_user_login)
        self.login_button.pack(side=tk.LEFT, padx=10)

        # Status Label
        self.login_status_label = tk.Label(self.login_frame, text="", fg="red")
        self.login_status_label.pack(pady=(10,0))

    def _create_main_widgets(self):
        self.main_frame = tk.Frame(self)

        # Status Bar
        self.status_bar = tk.Label(self.main_frame, text="Status: Disconnected", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        # Input Area Frame
        input_frame = tk.Frame(self.main_frame)
        input_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=5, padx=5)

        self.input_entry = tk.Entry(input_frame, width=50)
        self.input_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.input_entry.bind("<Return>", self._handle_send) # Bind Enter key

        self.send_button = tk.Button(input_frame, text="Send", command=self._handle_send)
        self.send_button.pack(side=tk.LEFT)

        # Online/Offline Buttons
        self.online_button = tk.Button(input_frame, text="Go Online", command=self._handle_go_online, state=tk.DISABLED)
        self.online_button.pack(side=tk.LEFT, padx=(5, 0))
        self.offline_button = tk.Button(input_frame, text="Go Offline", command=self._handle_go_offline, state=tk.DISABLED)
        self.offline_button.pack(side=tk.LEFT, padx=(5, 0))

        # Message Display Area
        self.message_area = scrolledtext.ScrolledText(self.main_frame, wrap=tk.WORD, state=tk.DISABLED, height=15)
        self.message_area.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def _update_status(self, text):
        """Safely update the status bar from any thread via queue."""
        gui_queue.put(("status", text)) # Use a tuple to signify a status update

    def _display_message(self, message):
        """Appends a message to the text area."""
        if not isinstance(message, str):
            message = str(message)
        self.message_area.configure(state=tk.NORMAL)
        self.message_area.insert(tk.END, message) # Append normally
        # Add newline if message doesn't end with one, except for prompt-like things
        if not message.endswith('\n') and not message.strip().endswith('>'):
             self.message_area.insert(tk.END, '\n')
        self.message_area.see(tk.END) # Scroll to bottom
        self.message_area.configure(state=tk.DISABLED)

    def _process_gui_queue(self):
        """Checks the queue for messages from backend and updates GUI."""
        try:
            while True:
                item = gui_queue.get_nowait()
                if isinstance(item, tuple) and item[0] == "status":
                    self.status_bar.config(text=f"Status: {item[1]}")
                    # Enable/disable online/offline buttons based on status
                    if "Offline" in item[1]:
                        self.online_button.config(state=tk.NORMAL)
                        self.offline_button.config(state=tk.DISABLED)
                    elif "Online" in item[1] or "Connected" in item[1]:
                         self.online_button.config(state=tk.DISABLED)
                         self.offline_button.config(state=tk.NORMAL)
                    else: # Disconnected, Connecting etc.
                         self.online_button.config(state=tk.DISABLED)
                         self.offline_button.config(state=tk.DISABLED)

                elif isinstance(item, tuple) and item[0] == "login_result":
                    success, message, user_info = item[1], item[2], item[3]
                    self._handle_login_response(success, message, user_info)
                elif isinstance(item, str):
                    self._display_message(item)
                else:
                     print(f"[GUI Queue] Unknown item type: {type(item)} - {item}")

        except queue.Empty:
            pass # No messages in queue is normal

        # Reschedule polling
        self.message_poll_job = self.after(100, self._process_gui_queue) # Poll every 100ms

    # --- Login Handlers ---
    def _attempt_login(self, name, user_id=None):
        """Starts the backend login process in a separate thread."""
        if not name:
            self.login_status_label.config(text="Name cannot be empty.")
            return

        login_type = "user" if user_id else "guest"
        self.login_status_label.config(text=f"Attempting {login_type} login as '{name}'...")
        self.guest_button.config(state=tk.DISABLED)
        self.login_button.config(state=tk.DISABLED)

        # Run backend's connection/login logic in a thread
        # Pass the necessary info and the queue
        self.backend_thread = threading.Thread(
            target=backend.try_initial_connection,
            args=(name, user_id, gui_queue), # Pass queue to backend function
            daemon=True
        )
        self.backend_thread.start()

    def _handle_guest_login(self):
        name = self.name_entry.get().strip()
        self._attempt_login(name) # ID is None for guest

    def _handle_user_login(self):
        name = self.name_entry.get().strip()
        user_id = self.id_entry.get().strip()
        if not user_id or not user_id.isdigit():
             self.login_status_label.config(text="Valid numeric ID required for user login.")
             return
        self._attempt_login(name, user_id)

    def _handle_login_response(self, success, message, user_info):
        """Handles the result of the login attempt from the backend."""
        self._display_message(f"[Login Attempt] {message}\n")
        if success:
            self.is_logged_in = True
            backend.MY_NAME = user_info.get('name') # Ensure backend knows name
            backend.MY_ID = user_info.get('id')
            backend.is_guest = user_info.get('is_guest')
            backend.MY_INFO = user_info.get('my_info')
            self.login_frame.pack_forget()
            self.main_frame.pack(fill=tk.BOTH, expand=True)
            self._update_status(f"Online as {backend.MY_NAME}")
            self.input_entry.focus_set()
            # Start the queue processor *after* successful login and frame switch
            self._process_gui_queue()
            # Start listeners (should be started by backend after successful ack)
            # Make sure backend.start_listeners(gui_queue) is called by try_initial_connection on success
            print("[GUI] Login successful, main view activated.")

        else:
            self.login_status_label.config(text=f"Login failed: {message}")
            self.guest_button.config(state=tk.NORMAL)
            self.login_button.config(state=tk.NORMAL)
            # If connection failed, backend thread might exit. No listeners started.
            self.backend_thread = None # Reset thread ref


    # --- Main View Handlers ---
    def _handle_send(self, event=None): # event=None allows calling without keybinding event
        """Sends the content of the input entry."""
        if not self.is_logged_in or backend.is_offline:
             self._display_message("[System] Cannot send while offline or not logged in.\n")
             return

        command = self.input_entry.get().strip()
        if not command:
            return

        self.input_entry.delete(0, tk.END) # Clear input field

        # Pass the raw command to the backend to handle parsing and execution
        # Run in a thread to avoid blocking GUI if backend action takes time
        # Although most commands are now non-blocking sends
        cmd_thread = threading.Thread(target=backend.handle_gui_command, args=(command, gui_queue), daemon=True)
        cmd_thread.start()

    def _handle_go_offline(self):
        if not self.is_logged_in or backend.is_offline:
            return
        print("[GUI] Requesting Go Offline...")
        # Tell backend to go offline (needs a function in backend)
        threading.Thread(target=backend.go_offline, args=(gui_queue,), daemon=True).start()
        self._update_status("Going offline...") # Immediate feedback


    def _handle_go_online(self):
        if not self.is_logged_in or not backend.is_offline:
             return
        print("[GUI] Requesting Go Online...")
         # Tell backend to go online (needs a function in backend)
        threading.Thread(target=backend.go_online, args=(gui_queue,), daemon=True).start()
        self._update_status("Attempting to go online...") # Immediate feedback

    def _on_closing(self):
        """Handles window close event."""
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
             print("[GUI] Quit requested by user.")
             # Stop queue polling
             if self.message_poll_job:
                 self.after_cancel(self.message_poll_job)
                 self.message_poll_job = None

             # Signal backend to shut down (nicely)
             if self.is_logged_in and backend.running:
                 # Run quit in a separate thread to avoid blocking GUI during shutdown
                 quit_thread = threading.Thread(target=backend.handle_gui_command, args=("/quit", gui_queue), daemon=True)
                 quit_thread.start()
                 # Give backend a moment to process quit
                 time.sleep(0.7) # Adjust as needed

             # Force close backend resources if still running after delay
             backend.force_shutdown()

             self.destroy()
             print("[GUI] Window destroyed.")
             # Ensure the script exits fully if backend threads are stubborn
             # sys.exit(0) # Use cautiously, might cut off backend cleanup

# --- Modify Backend ---
# Add necessary functions to p2p_client_backend.py to integrate with the GUI

def try_initial_connection(name, user_id, queue_to_gui):
    """
    Backend function called by GUI to attempt login.
    Communicates result back via the queue.
    Starts listeners on success.
    """
    global gui_queue_ref # Store queue reference if needed globally in backend
    gui_queue_ref = queue_to_gui
    login_success = False
    ack_message = "Login process not completed."
    final_user_info = {}

    # Reuse parts of the original login_or_guest logic
    login_data = None
    potential_login_info = {}
    is_guest_login = (user_id is None)

    try:
        if is_guest_login:
            if name and 0 < len(name) <= 50 and ' ' not in name:
                # Set backend globals (important!)
                backend.MY_NAME = name
                backend.MY_ID = None
                backend.is_guest = True
                backend.update_my_info(backend.MY_NAME, backend.MY_P2P_PORT)
                login_data = {'type': 'guest_login', 'name': backend.MY_NAME, 'ip': backend.MY_INFO.get('ip'), 'p2p_port': backend.MY_P2P_PORT, 'is_invisible': backend.is_invisible}
                potential_login_info = {'type': 'guest', 'name': backend.MY_NAME}
            else:
                 ack_message = "Invalid guest name (1-50 chars, no spaces)."
                 queue_to_gui.put(("login_result", False, ack_message, {}))
                 return # Exit if name invalid
        else: # User login
             if name and 0 < len(name) <= 50 and ' ' not in name and user_id and user_id.isdigit():
                backend.MY_NAME = name
                backend.MY_ID = user_id
                backend.is_guest = False
                backend.update_my_info(backend.MY_NAME, backend.MY_P2P_PORT)
                login_data = {'type': 'user_login', 'name': backend.MY_NAME, 'id': backend.MY_ID, 'ip': backend.MY_INFO.get('ip'), 'p2p_port': backend.MY_P2P_PORT, 'is_invisible': backend.is_invisible}
                potential_login_info = {'type': 'user', 'name': backend.MY_NAME, 'id': backend.MY_ID}
             else:
                  ack_message = "Invalid user name or ID format."
                  queue_to_gui.put(("login_result", False, ack_message, {}))
                  return # Exit if invalid

        # --- Connection and Ack Logic (from login_or_guest) ---
        temp_socket = None
        try:
            print(f"[BACKEND] Connecting to Registry {backend.REGISTRY_HOST}:{backend.REGISTRY_PORT}...")
            temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_socket.settimeout(10.0)
            temp_socket.connect((backend.REGISTRY_HOST, backend.REGISTRY_PORT))
            print("[BACKEND] Connected. Sending identification...")
            temp_socket.sendall((json.dumps(login_data) + "\n").encode('utf-8'))

            print("[BACKEND] Waiting for acknowledgement...")
            buffer = ""; ack_received = False; temp_socket.settimeout(20.0) # Adjust timeout?
            while not ack_received and backend.running: # Check backend.running
                try:
                    data = temp_socket.recv(backend.BUFFER_SIZE)
                    if not data:
                        ack_message = "Connection closed by server during ACK."; ack_received=True; break
                    buffer += data.decode('utf-8')
                    while '\n' in buffer and not ack_received: # Process all messages in buffer
                        resp_json, buffer = buffer.split('\n', 1)
                        if not resp_json.strip(): continue
                        response = json.loads(resp_json); r_type = response.get('type')
                        print(f"[BACKEND RECV] Ack-related msg: {r_type}") # Debug print
                        if r_type == 'login_ack' or r_type == 'guest_ack':
                            ack_message = response.get('message', 'Login successful.')
                            server_ip = response.get('client_ip')
                            if server_ip: backend.update_my_info(backend.MY_NAME, backend.MY_P2P_PORT, ip=server_ip)
                            login_success = True
                            backend.registry_socket = temp_socket # Assign the connected socket
                            backend.registry_socket.settimeout(None) # Important: remove timeout for listener
                            backend.initial_login_info = potential_login_info # Store for reconnect
                            # Prepare user info to send back to GUI
                            final_user_info = {
                                'name': backend.MY_NAME,
                                'id': backend.MY_ID,
                                'is_guest': backend.is_guest,
                                'my_info': backend.MY_INFO.copy() # Send a copy
                            }
                            ack_received = True # Exit outer loop
                            break # Exit inner loop
                        elif r_type == 'error':
                            ack_message = response.get('message', 'Unknown server error.')
                            ack_received = True; break
                        else:
                            # Handle other messages received during login (like peer lists?)
                            # This might require routing them through the normal listener logic later
                            print(f"[BACKEND WARNING] Unexpected message during ACK wait: {r_type}")
                            # Maybe put unexpected messages onto the queue for GUI display?
                            queue_to_gui.put(f"[Server Early Msg] {resp_json}")

                except socket.timeout: ack_message = "Timeout waiting for server acknowledgement."; ack_received=True; break
                except (socket.error, json.JSONDecodeError, UnicodeDecodeError) as e: ack_message = f"Acknowledgement error: {e}"; ack_received=True; break
                except Exception as e: ack_message = f"Critical ACK error: {e}"; ack_received=True; backend.running = False; break # Critical failure

            if not login_success and temp_socket:
                try: temp_socket.close()
                except: pass
                backend.registry_socket = None

        except socket.timeout: ack_message = f"Connection to {backend.REGISTRY_HOST}:{backend.REGISTRY_PORT} timed out."
        except socket.error as e: ack_message = f"Registry connection error: {e}"
        except Exception as e: ack_message = f"Critical Login error: {e}"; backend.running = False # Critical failure

    finally:
        # Send result back to GUI
        queue_to_gui.put(("login_result", login_success, ack_message, final_user_info))

        if login_success and backend.running:
            # Start listener threads only after successful login and ACK
            print("[BACKEND] Login successful. Starting listeners...")
            backend.start_listeners(queue_to_gui) # Pass queue to listener starter
            backend.is_offline = False # Ensure state is online
        elif not login_success:
            print(f"[BACKEND] Login failed: {ack_message}")
            # Ensure backend state reflects failure
            backend.running = False # Or set a specific 'login_failed' state?


def start_listeners(queue_to_gui):
    """Starts the P2P and Registry listener threads."""
    if not backend.running:
        print("[BACKEND ERROR] Cannot start listeners, backend not running.")
        return

    # --- Ensure previous threads are stopped if any (e.g., after reconnect) ---
    stop_and_join_thread(backend.p2p_listener_thread, backend.stop_p2p_listener_event)
    stop_and_join_thread(backend.registry_listener_thread, backend.stop_registry_listener_event)

    # --- Start New Listeners ---
    backend.stop_p2p_listener_event = threading.Event()
    backend.stop_registry_listener_event = threading.Event()

    print(f"[BACKEND] Starting P2P listener on {backend.LISTEN_HOST}:{backend.MY_P2P_PORT}...")
    backend.p2p_listener_thread = threading.Thread(
        target=backend.listen_for_peers_wrapper, # Use wrapper
        args=(backend.LISTEN_HOST, backend.MY_P2P_PORT, backend.stop_p2p_listener_event, queue_to_gui),
        daemon=True
    )
    backend.p2p_listener_thread.start()
    time.sleep(0.1) # Brief pause

    print("[BACKEND] Starting Registry listener...")
    if backend.registry_socket:
        backend.registry_listener_thread = threading.Thread(
            target=backend.listen_to_registry_wrapper, # Use wrapper
            args=(backend.registry_socket, backend.stop_registry_listener_event, queue_to_gui),
            daemon=True
        )
        backend.registry_listener_thread.start()
    else:
        print("[BACKEND CRITICAL] Registry socket unavailable after login. Cannot start registry listener.")
        queue_to_gui.put("[ERROR] Registry connection lost. Functionality limited.")
        backend.is_offline = True # Force offline if registry listener fails
        queue_to_gui.put(("status", f"Error - Connection Lost ({backend.MY_NAME})"))

def stop_listeners():
    """Signals listener threads to stop."""
    print("[BACKEND] Stopping listeners...")
    if backend.stop_p2p_listener_event:
        backend.stop_p2p_listener_event.set()
    if backend.stop_registry_listener_event:
        backend.stop_registry_listener_event.set()

def stop_and_join_thread(thread, stop_event):
    """Helper to signal and join a thread."""
    if thread and thread.is_alive():
        if stop_event:
            stop_event.set()
        print(f"[BACKEND] Waiting for thread {thread.name} to finish...")
        thread.join(timeout=1.0) # Wait max 1 second
        if thread.is_alive():
            print(f"[BACKEND WARNING] Thread {thread.name} did not finish cleanly.")

# --- Wrappers for Listener Functions ---
# These wrappers capture print statements within the thread execution

def listen_for_peers_wrapper(host, port, stop_event, queue_to_gui):
    """Wrapper for P2P listener to redirect output."""
    # Redirect within the thread's context if needed, though global redirect might suffice
    # sys.stdout = TextRedirector(queue_to_gui, "stdout")
    # sys.stderr = TextRedirector(queue_to_gui, "stderr")
    print(f"[Thread-{threading.current_thread().name}] P2P listener started.")
    try:
        backend.listen_for_peers(host, port, stop_event)
    except Exception as e:
        print(f"[P2P Listener Thread CRASH] {e}") # Use print to send to queue
    finally:
        print(f"[Thread-{threading.current_thread().name}] P2P listener finished.")

def listen_to_registry_wrapper(reg_socket, stop_event, queue_to_gui):
    """Wrapper for Registry listener to redirect output."""
    # Redirect within the thread's context if needed
    # sys.stdout = TextRedirector(queue_to_gui, "stdout")
    # sys.stderr = TextRedirector(queue_to_gui, "stderr")
    print(f"[Thread-{threading.current_thread().name}] Registry listener started.")
    try:
        backend.listen_to_registry(reg_socket, stop_event)
    except Exception as e:
        print(f"[Registry Listener Thread CRASH] {e}") # Use print to send to queue
    finally:
        print(f"[Thread-{threading.current_thread().name}] Registry listener finished.")


def handle_gui_command(command, queue_to_gui):
    """Handles commands entered into the GUI input field."""
    if not backend.running:
         print("[BACKEND] Cannot handle command, not running.")
         return

    cmd_lower = command.lower().strip()
    print(f"({backend.MY_NAME})> {command}") # Echo command using redirected print

    # --- Quit Command ---
    if cmd_lower == '/quit':
        print("[BACKEND] Quit command received from GUI.")
        if not backend.is_offline and backend.registry_socket:
             try:
                  # Notify server about going offline status before quitting
                  offline_status_msg = {'type': 'update_offline_status', 'is_offline': True}
                  backend.registry_socket.sendall((json.dumps(offline_status_msg) + "\n").encode('utf-8'))
                  print("[BACKEND] Sent final offline status to registry.")
             except Exception as e:
                  print(f"[BACKEND WARN] Failed to send final offline status: {e}")
        # Signal listeners to stop
        stop_listeners()
        # Close registry socket
        if backend.registry_socket:
            print("[BACKEND] Closing registry socket...")
            try: backend.registry_socket.close()
            except Exception: pass
            backend.registry_socket = None
        backend.running = False # Set running flag to false
        queue_to_gui.put(("status", "Shutting down..."))
        print("[BACKEND] Shutdown initiated.")
        # GUI's _on_closing will handle window destruction

    # --- Offline State Handling ---
    elif backend.is_offline:
        if cmd_lower == '/online':
            go_online(queue_to_gui) # Call the specific function
        # Handle allowed offline commands (mirroring the CLI logic)
        elif cmd_lower == '/myinfo': print(f"[YOUR INFO]: {backend.MY_INFO} | Status: {'Guest' if backend.is_guest else f'User (ID: {backend.MY_ID})'} | State: OFFLINE")
        elif cmd_lower == '/my_channels':
            print("[YOUR CHANNELS (Local View)]:")
            with backend.my_channels_lock: channels=sorted(backend.my_channels.items())
            if channels: [print(f"  - {n} ({'Owner' if d.get('owner')==backend.MY_NAME else 'Member'}, Owner: {d.get('owner','?')}, {len(d.get('members',{})) if d.get('owner')==backend.MY_NAME else '?'} members)") for n,d in channels]
            else: print("  (Not in any channels locally)")
        elif cmd_lower.startswith('/history '):
            parts=command.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
            if name: backend.show_local_history(name) # Assume function exists or implement
            else: print("[CMD ERROR] Usage: /history <channel_name>")
        elif cmd_lower.startswith('/members '):
            parts=command.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
            if name: backend.show_local_members(name) # Assume function exists or implement
            else: print("[CMD ERROR] Usage: /members <channel_name>")
        elif cmd_lower.startswith('/msg '): # Queues offline message
            parts=command.split(' ',2)
            if len(parts)==3:
                channel_name=parts[1].strip()
                msg_content=parts[2]
                if channel_name and msg_content:
                    # Queue message using backend logic (pass None for socket)
                    backend.send_channel_msg(channel_name, msg_content, None)
                else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")
            else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")
        elif cmd_lower == '/invisible':
             backend.is_invisible = not backend.is_invisible; status = "invisible" if backend.is_invisible else "visible"; print(f"[STATUS] Visibility set to {status}. Will apply when next online.")
        else: print("[INFO] You are offline. Available: /online, /quit, /myinfo, /my_channels, /members, /history, /msg, /invisible.")

    # --- Online State Commands ---
    elif cmd_lower == '/offline':
        go_offline(queue_to_gui) # Call the specific function

    elif cmd_lower == '/invisible':
        backend.is_invisible = not backend.is_invisible; status = "invisible" if backend.is_invisible else "visible"; print(f"[STATUS] You are now {status}")
        try: # Notify server
            if backend.registry_socket: backend.registry_socket.sendall((json.dumps({'type': 'update_visibility', 'is_invisible': backend.is_invisible}) + "\n").encode('utf-8'))
            else: print("[ERROR] Cannot update visibility: Offline.")
        except Exception as e: print(f"[ERROR] Failed to update visibility status: {e}"); backend.is_invisible = not backend.is_invisible

    elif cmd_lower == '/list':
        print("[CMD] Requesting peer list...")
        try:
             if backend.registry_socket: backend.registry_socket.sendall((json.dumps({'type':'list','peer_id':f"user_{backend.MY_P2P_PORT}" if not backend.is_guest else f"guest_{backend.MY_P2P_PORT}"})+"\n").encode('utf-8'))
             else: print("[ERROR] Cannot list peers: Offline.")
        except Exception as e: print(f"[ERROR] Failed to send list request: {e}")

    elif cmd_lower == '/myinfo': print(f"[YOUR INFO]: {backend.MY_INFO} | Status: {'Guest' if backend.is_guest else f'User (ID: {backend.MY_ID})'} | State: ONLINE")

    elif cmd_lower.startswith('/msg '): # Sends online message
        parts=command.split(' ',2)
        if len(parts)==3:
            channel_name=parts[1].strip();
            msg_content=parts[2]
            if channel_name and msg_content: backend.send_channel_msg(channel_name,msg_content, backend.registry_socket) # Pass socket
            else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")
        else: print("[CMD ERROR] Usage: /msg <channel_name> <message>")

    elif cmd_lower.startswith('/create '):
        parts=command.split(' ',2)
        if len(parts)==3:
            name=parts[1].strip()
            channel_type=parts[2].strip()
            if name and channel_type in ['public', 'private']: backend.create_channel(backend.registry_socket, name, channel_type)
            else: print("[CMD ERROR] Usage: /create <channel_name> <public|private>")
        else: print("[CMD ERROR] Usage: /create <channel_name> <public|private>")

    elif cmd_lower.startswith('/join '):
         parts=command.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
         if name: backend.join_channel(name)
         else: print("[CMD ERROR] Usage: /join <channel_name>")

    elif cmd_lower == '/list_channels':
         print("[CMD] Requesting channel list...")
         try:
              if backend.registry_socket: backend.registry_socket.sendall((json.dumps({'type': 'request_channel_list'}) + "\n").encode('utf-8'))
              else: print("[ERROR] Cannot list channels: Offline.")
         except Exception as e: print(f"[ERROR] Sending request: {e}")

    elif cmd_lower == '/my_channels': # Same as offline
        print("[YOUR CHANNELS (Local View)]:")
        with backend.my_channels_lock: channels=sorted(backend.my_channels.items())
        if channels: [print(f"  - {n} ({'Owner' if d.get('owner')==backend.MY_NAME else 'Member'}, Owner: {d.get('owner','?')}, {len(d.get('members',{})) if d.get('owner')==backend.MY_NAME else '?'} members)") for n,d in channels]
        else: print("  (Not in any channels locally)")

    elif cmd_lower.startswith('/members '): # Same as offline
         parts=command.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
         if name: backend.show_local_members(name) # Assume function exists
         else: print("[CMD ERROR] Usage: /members <channel_name>")

    elif cmd_lower.startswith('/history '): # Same as offline
        parts=command.split(' ',1); name=parts[1].strip() if len(parts)==2 else None
        if name: backend.show_local_history(name) # Assume function exists
        else: print("[CMD ERROR] Usage: /history <channel_name>")

    else: print("[CMD ERROR] Unknown command.")

# --- Add helper functions for offline/online toggling ---

def go_offline(queue_to_gui):
    """Backend logic to transition to offline state."""
    if not backend.running or backend.is_offline:
        return
    print("[BACKEND] Going offline...")
    backend.is_offline = True # Set state first
    try: # Notify server
        if backend.registry_socket:
            backend.registry_socket.sendall((json.dumps({'type': 'update_offline_status', 'is_offline': True}) + "\n").encode('utf-8'))
            print("[BACKEND] Offline status notification sent.")
        else:
            print("[BACKEND WARNING] Cannot send offline status: No registry connection.")
    except Exception as e:
        print(f"[BACKEND WARNING] Failed to send offline status notification: {e}")

    # Stop listeners
    stop_listeners()

    # Close registry socket (important!)
    if backend.registry_socket:
        print("[BACKEND] Closing registry connection for offline mode...")
        try: backend.registry_socket.close()
        except Exception: pass
        backend.registry_socket = None

    # Wait for threads to potentially finish
    stop_and_join_thread(backend.p2p_listener_thread, None) # Event was already set
    stop_and_join_thread(backend.registry_listener_thread, None)

    backend.p2p_listener_thread = None
    backend.registry_listener_thread = None

    queue_to_gui.put(("status", f"Offline ({backend.MY_NAME})"))
    print("[BACKEND] Now offline. Network listeners stopped.")

def go_online(queue_to_gui):
    """Backend logic to transition back to online state."""
    if not backend.running or not backend.is_offline:
        return
    print("[BACKEND] Attempting to go online...")
    queue_to_gui.put(("status", "Reconnecting..."))

    # Use the reconnect logic
    reconnected = backend.reconnect_and_register(backend.REGISTRY_HOST, backend.REGISTRY_PORT)

    if reconnected:
        print("[BACKEND] Reconnected successfully.")
        backend.is_offline = False # Update state
        try: # Send online status update
            if backend.registry_socket:
                backend.registry_socket.sendall((json.dumps({'type': 'update_offline_status', 'is_offline': False}) + "\n").encode('utf-8'))
                print("[BACKEND] Online status update sent.")
            else:
                 print("[BACKEND ERROR] Reconnected but registry socket missing for status update.")
        except Exception as e:
            print(f"[BACKEND ERROR] Failed to send online status update after reconnect: {e}")

        # Restart listeners
        start_listeners(queue_to_gui) # Pass queue

        # Sync offline messages
        time.sleep(0.2) # Allow listeners to fully start
        print("[BACKEND] Synchronizing data...")
        # Run sync in background thread? Might take time
        threading.Thread(target=backend.synchronize_host_server, args=(backend.registry_socket,), daemon=True).start()
        threading.Thread(target=backend.synchronize_offline_messages, args=(backend.registry_socket,), daemon=True).start()

        queue_to_gui.put(("status", f"Online as {backend.MY_NAME}"))
        print("[BACKEND] Back online and synchronizing.")
    else:
        print("[BACKEND] Failed to go online.")
        backend.is_offline = True # Stay offline
        queue_to_gui.put(("status", f"Offline (Reconnect Failed) ({backend.MY_NAME})"))

def show_local_history(channel_name):
    """Helper to print local history (used by handle_gui_command)."""
    if not backend.running: return
    is_owner=False
    with backend.my_channels_lock:
        is_owner = channel_name in backend.my_channels and backend.my_channels[channel_name].get('owner')==backend.MY_NAME
    if is_owner:
        print(f"[HISTORY for '{channel_name}'] (Stored locally by owner):")
        log_entries=[]
        with backend.channel_logs_lock:
            log_entries=backend.channel_message_logs.get(channel_name,[]).copy()
        if log_entries:
            for entry in log_entries:
                ts=entry.get('timestamp','?'); s=entry.get('sender','?'); c=entry.get('content','')
                try:
                    t_obj=datetime.datetime.fromisoformat(ts);
                    t_str=t_obj.strftime('%Y-%m-%d %H:%M:%S') # Use backend's datetime
                except: t_str=ts
                print(f"  [{t_str}] {s}: {c}") # Goes to GUI via redirect
        else: print("  (No messages logged locally)")
    else: print(f"[CMD ERROR] Can only view history for channels you own.")

def show_local_members(channel_name):
     """Helper to print local members (used by handle_gui_command)."""
     if not backend.running: return
     with backend.my_channels_lock: channel_data=backend.my_channels.get(channel_name)
     if channel_data:
         print(f"[MEMBERS of '{channel_name}'] (Local View):")
         members=sorted(channel_data.get('members', {}).keys())
         if members:
             for m in members:
                 owner_tag = '(Owner)' if m == channel_data.get('owner') else ''
                 you_tag = '(You)' if m == backend.MY_NAME else ''
                 print(f"  - {m} {owner_tag} {you_tag}") # Goes to GUI
         else: print("  (No members known locally)")
     else: print(f"[CMD ERROR] Not part of channel '{channel_name}' locally.")

def force_shutdown():
    """Called by GUI on close if backend doesn't stop cleanly."""
    print("[BACKEND] Force shutdown called by GUI.")
    backend.running = False
    stop_listeners()
    if backend.registry_socket:
        try: backend.registry_socket.close()
        except: pass
    # Add any other forceful cleanup if necessary


# --- Add the new functions to the backend namespace ---
# This makes them callable like backend.try_initial_connection(...)
backend.try_initial_connection = try_initial_connection
backend.start_listeners = start_listeners
backend.stop_listeners = stop_listeners
backend.handle_gui_command = handle_gui_command
backend.go_offline = go_offline
backend.go_online = go_online
backend.show_local_history = show_local_history
backend.show_local_members = show_local_members
backend.force_shutdown = force_shutdown
backend.listen_for_peers_wrapper = listen_for_peers_wrapper
backend.listen_to_registry_wrapper = listen_to_registry_wrapper

# --- Add necessary imports to backend if missing ---
# Make sure p2p_client_backend.py imports 'socket', 'threading', 'sys', 'time', 'json', 'random', 'datetime', 'queue'
# Specifically add 'socket', 'json', 'threading', 'time', 'datetime' if not already globally imported in the backend script for these new functions.
import socket
import datetime # Make sure backend has this

# --- Start GUI ---
if __name__ == "__main__":
    app = P2PClientGUI()
    app.mainloop()
    print("[GUI] Mainloop finished.")
    # Explicit exit might be needed if backend threads don't terminate properly
    # sys.exit(0)