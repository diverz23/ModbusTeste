from socket import *
import asyncio, struct,time
import pymysql as sql


db = sql.connect(host='192.168.1.102', user = 'seabra', passwd = 'junior23', db = 'miralago')
cur = db.cursor()

cur.execute("SELECT * FROM tbllog;")
results = cur.fetchall()

print(results)
db.close()

loop = asyncio.get_event_loop()

async def modbus_Server(address):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET,SO_REUSEADDR,1)
    sock.bind(address)
    sock.listen(5)
    sock.setblocking(False)
    while True:
        client, addr = await loop.sock_accept(sock)
        print ("Connection with:", addr )
        loop.create_task(modbus_handler(client))

async def modbus_handler(client):
    with client:
        while True:
            data = await loop.sock_recv(client, 10000)
            print("Dados Recebidos",data)
            Modbusrequest(data)
            sendDadosDB(data)
            if not data:
                client.close()
                break
            await loop.sock_sendall(client,b'got:' + data)
    print("Connection closed")

def Modbusrequest(data):
    try:
        dados = struct.unpack('!HHHBBHH', data)
        TransactionID = dados[0]
        Protocolo = dados[1]
        MensagemLengh = dados[2]
        UnitIdentifier = dados[3]
        FunctionCode= dados[4]
        EnderecoFirstregister = dados[5]
        NumeroRegistos = dados[6]
    except struct.error as error:
        print(error)

    print('Unidade' + str(UnitIdentifier))


loop.create_task(modbus_Server(('',502)))
loop.run_forever()




