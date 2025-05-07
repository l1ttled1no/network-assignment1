# P2P Chat Application

## Table of Contents

1.  [Description](#description)
2.  [Features](#features)
3.  [Components](#components)
    - [Server](#server-serverpy)
    - [Command-Line Client](#command-line-client-p2p_clientpy)
    - [GUI Client](#gui-client-p2p_client_guipy)
4.  [Setup and Installation](#setup-and-installation)
5.  [Usage](#usage)
    - [Running the Server](#running-the-server)
    - [Running the Command-Line Client](#running-the-command-line-client)
    - [Running the GUI Client](#running-the-gui-client)
6.  [Command-Line Client Commands](#command-line-client-commands)
7.  [GUI Client Instructions](#gui-client-instructions)

## Description

This project is a Peer-to-Peer (P2P) Chat Application that allows users to communicate directly with each other after registering with a central server. It supports user and guest logins, public and private chat channels, peer discovery, offline message queuing, and an invisible mode. The application includes both a command-line interface (CLI) client and a graphical user interface (GUI) client built with Tkinter.

## Features

- **User Authentication:** Supports registered user logins (name and ID) and guest logins (name only).
- **Peer Discovery:** Clients can request a list of currently online and visible peers from the server.
- **Channel Management:**
  - Users can create public or private chat channels.
  - Users can list available channels and join them.
- **P2P Messaging:** Direct communication between clients within channels.
  - **Owner-based Forwarding:** In channels, messages from members are sent to the channel owner, who then broadcasts them to all other members. Owners also log messages.
- **Server-Side Logging & Synchronization:** The server logs messages for channels, allowing clients to synchronize and retrieve history (especially for channel owners).
- **Offline Mode:** Clients can go offline. Messages sent while offline are queued and sent upon reconnecting.
- **Invisible Mode:** Users can set their status to "invisible" to be hidden from peer lists while still being able to use the application.
- **Dual Clients:**
  - **Command-Line Interface (CLI):** A text-based client (`p2p_client.py`).
  - **Graphical User Interface (GUI):** A user-friendly Tkinter-based client (`p2p_client_gui.py`).
- **Real-time Updates:** The server pushes updates to clients (e.g., new peers, new channels, new messages via `notify_new_msg`).

## Components

### Server (`server.py`)

The central registry server responsible for:

- Managing user and guest registrations and authentications.
- Tracking connected peers and their status (online, offline, invisible).
- Registering new chat channels and maintaining a list of available channels.
- Storing message history for channels to facilitate synchronization.
- Broadcasting updates about new peers and channels to connected clients.
- Handling client disconnections gracefully.

### Command-Line Client (`p2p_client.py`)

A text-based client application that allows users to:

- Connect to the server and log in as a user or guest.
- Listen for incoming P2P connections from other clients.
- Interact with the chat system using text commands (see [Command-Line Client Commands](#command-line-client-commands)).
- Manage local channel state, including membership and message logs for owned channels.
- Queue messages when offline and synchronize upon reconnection.

### GUI Client (`p2p_client_gui.py`)

A graphical client built with Tkinter, providing a more user-friendly interface for all client functionalities:

- Login dialog for user/guest authentication.
- Display areas for:
  - List of online peers.
  - Channels the user is currently a member of ("My Channels").
  - List of all available channels on the server.
  - Chat messages for the selected channel.
- Input field for sending messages.
- Buttons for actions like listing peers/channels, creating/joining channels, and managing online/offline/invisible status.
- Status bar indicating current user, connection status, visibility, and active channel.
- Notifications for new messages in non-active channels.

## Setup and Installation

1.  **Python:** Ensure you have Python 3.x installed (Python 3.6 or newer is recommended).
2.  **Dependencies:** The project uses standard Python libraries:
    - `socket`
    - `threading`
    - `json`
    - `time`
    - `sys`
    - `random`
    - `datetime`
    - `queue` (for GUI client)
    - `tkinter` (for GUI client, usually included with Python standard installations)
      No external packages need to be installed via pip if you have a standard Python distribution.

## Usage

### Running the Server

1.  Open a terminal or command prompt.
2.  Navigate to the directory containing `server.py`.
3.  Run the server script:
    ```bash
    python server.py
    ```
4.  The server will start and listen for incoming client connections, by default on `0.0.0.0:9999`.

### Running the Command-Line Client

1.  Open a new terminal or command prompt.
2.  Navigate to the directory containing `p2p_client.py`.
3.  Run the client script:
    ```bash
    python p2p_client.py
    ```
4.  Follow the on-screen prompts to log in (e.g., `/guest Alice` or `/login Bob 123`).
5.  Use the available commands (see below) to interact with the chat system.

### Running the GUI Client

1.  Open a new terminal or command prompt.
2.  Navigate to the directory containing `p2p_client_gui.py`.
3.  Run the GUI client script:
    ```bash
    python p2p_client_gui.py
    ```
4.  A login window will appear. Enter your username (and ID if logging in as a registered user) or choose to log in as a guest.
5.  Once logged in, the main chat interface will be displayed.

## Command-Line Client Commands

The CLI client (`p2p_client.py`) supports the following commands:

- `/guest <name>`: Log in as a guest with the specified name.
- `/login <name> <id>`: Log in as a registered user with the specified name and ID.
- `/list`: Request and display the list of online and visible peers.
- `/myinfo`: Display your current user information (name, ID, IP, port, status).
- `/msg <channel_name> <message>`: Send a message to the specified channel.
- `/create <channel_name> <public|private>`: Create a new channel.
- `/join <channel_name>`: Join an existing channel.
- `/list_channels`: Request and display the list of all available channels.
- `/my_channels`: Display the list of channels you are currently a member of.
- `/members <channel_name>`: Display the members of a channel you are in.
- `/history <channel_name>`: Display the locally stored message history for a channel you own.
- `/invisible`: Toggle your visibility status. When invisible, you won't appear in peer lists.
- `/offline`: Disconnect from the server and go into offline mode. Messages can be queued.
- `/online`: Attempt to reconnect to the server if currently offline.
- `/quit`: Disconnect and exit the client application.

## GUI Client Instructions

The GUI client (`p2p_client_gui.py`) provides an intuitive interface:

- **Login:** Enter your username (and ID for registered users) in the initial dialog.
- **Peer List:** Displays online and visible peers.
- **My Channels List:** Shows channels you've joined. Click to select a channel for chatting. New messages in unselected channels will be indicated.
- **Available Channels List:** Shows all channels on the server. Double-click a channel to attempt to join it.
- **Chat Area:** Displays messages for the currently selected channel.
- **Message Input:** Type your message and press Enter or click "Send".
- **Buttons:**
  - **List Peers:** Refreshes the peer list.
  - **List Channels:** Refreshes the available channels list.
  - **Create:** Opens a dialog to create a new channel (name and type).
  - **Join:** Opens a dialog to join an existing channel by name.
  - **Go Invisible/Visible:** Toggles your visibility.
  - **Go Online/Offline:** Manages your connection state with the server.
- **Status Bar:** Shows your name, online/offline status, visibility, and the currently active channel.
