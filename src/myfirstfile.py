import socket

host = "127.0.0.1"
port = 8080
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((host, port))
server_socket.listen(5)
print(f"Server is listening on {host}:{port}")

client_socket, client_address = server_socket.accept()
print(f"Accepted connection from {client_address}")

# Kommentar
while True:

    data = client_socket.recv(1024)

    if not data:
        break

    received_data = data.decode('utf-8')
    print(f" Received message : { received_data }")
   
    #binary_response = b"Binary response from server."
    #client_socket.send(binary_response)

    response = "Server hat " + received_data + " erhalten"
    client_socket.send(response.encode('utf-8'))

client_socket.close()
server_socket.close()

print("Verbindungen geschlossen")
print("Programm Ende")

