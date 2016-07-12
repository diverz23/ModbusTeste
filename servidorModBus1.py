from socket import *
import asyncio, struct,time
import pymysql as sql
EnderecoServidor = '100.100.150.50'
UnidadeCliente = 1

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
            #print("Dados Recebidos",data)
            ModbusRequest(data)
            #sendDadosDB(data)
            if not data:
                client.close()
                break
            await loop.sock_sendall(client,data)
    print("Connection closed")

def ModbusRequest(data):

    try:
        dados = struct.unpack('!HHHBBHH', data)
        TransactionID = dados[0]
        Protocolo = dados[1]
        MensagemLengh = dados[2]
        UnitIdentifier = dados[3]
        MBAP = dados[0:4]
        #verificar se a mensagem é válida
        reply = MODBUS_PDU_Checking(MBAP)
        #se a mensagem estiver OK emtão processa o resto dos dados
        if reply == 1:

            #print(''.join(map(str,dados)))
            #print (MBAP)
            #print(UnitIdentifier)
            FunctionCode = dados[4]
            EnderecoFirstRegister = dados[5]

            sql_log(''.join(map(str, dados)), ''.join(map(str, MBAP)), UnitIdentifier,
                    FunctionCode, EnderecoFirstRegister,dados[6])
            # verificar função solicitada
            if FunctionCode == 3:
                NumeroRegistos = dados[6]
                mensagem_leitura = {'TransactionID': TransactionID,
                                    'MensagemLengh': MensagemLengh,
                                    'UnitIdentifier': UnitIdentifier,
                                    'FunctionCode': FunctionCode,
                                    'EnderecoFirstRegister': EnderecoFirstRegister,
                                    'NumeroRegistos': NumeroRegistos}
                MODBUS_SERVICE_Processing(mensagem_leitura)
                # mensagam_recebida = dict(dados)
            elif FunctionCode == 6:
                valor = dados[6]
                mensagem_escrita = {'TransactionID': TransactionID,
                                    'MensagemLengh': MensagemLengh,
                                    'UnitIdentifier': UnitIdentifier,
                                    'FunctionCode': FunctionCode,
                                    'EnderecoFirstRegister': EnderecoFirstRegister,
                                    'Dados': valor}
                MODBUS_SERVICE_Processing(mensagem_escrita)
            else:
                print('Função não suportada')

        elif reply ==0:
            print ('Erro na  mensagem')

    except struct.error as error:
        print(error)

def MODBUS_PDU_Checking (MBAP):
    #Verificar se o protocolo está correctamente identificado
    if MBAP[1] == 0:
        return 1
    else:
        return 0

#processamento da mensagem
def MODBUS_SERVICE_Processing(msg):
    print ('Função : ' , msg['FunctionCode'])
    if msg['UnitIdentifier'] == 1:
        maquinaCliente = '23117'
        if msg['FunctionCode'] == 6:
            print('Dados: ' ,msg['Dados'])
            print ('Endereço registo: ', msg['EnderecoFirstRegister'])
            if msg['EnderecoFirstRegister'] == 1:
                #O primeiro endereco trata da horas de trabalho do motor
                Horas_Motor(msg['Dados'],maquinaCliente)
            elif msg['EnderecoFirstRegister'] == 3:
                Tempo_Automatico(msg['Dados'],maquinaCliente)

        elif msg['FunctionCode'] == 3:
            print('Número de registos solicitados: ', msg['NumeroRegistos'])
        #print('A mensagem é ', msg)
        msg.clear()
    else:
        print('Unidade Desconhecida')

#registo de tempo de horas motor
def Horas_Motor(dados,maquina):
    print('Horas Funcionamento motor %s na máquina %s' %(dados,maquina))
    #sql_dados(dados)

def Tempo_Automatico(dados, maquina):
    print('Tempo Automatico é de %s na máquina %s' %(dados,maquina))

def sql_dados (dados):
    db = sql.connect(host=EnderecoServidor, user='seabra', passwd='junior45', db='miralago')
    cur = db.cursor()

    cur.execute("SELECT * FROM tbllog;")
    results = cur.fetchall()

    #print(results)
    db.commit()
    db.close()

def sql_log(msg,MBAP,Slave,Function, FirstRegister, Data):
    db = sql.connect(host= EnderecoServidor, user='seabra', passwd='junior45', db='miralago')
    cur = db.cursor()
    #print(msg ,MBAP, Slave, Function, FirstRegister, Data)

    try:
        cur.execute ("INSERT INTO tblLog (Mensagem,MBAP,IDSlave,MBFunction,PrimeiroEndereco,Dados) VALUES"
                     " ( '{:0>7}', '{:0>4}' ,'{:0>2}', '{:0>2}','{:0>2}','{:0>10}');".format(msg,MBAP,Slave,Function,FirstRegister,Data))
        print (cur._last_executed)
        #print('Dados enviados correctamente')
        db.commit()

    except cur.DatabaseError as Dberror:
        print (Dberror)
        print(cur._last_executed)
        #print('Erro no envio de dados')
    except:
        print(cur._last_executed)
        print('Erro no envio de dados')

    db.close()

loop.create_task(modbus_Server(('',502)))
loop.run_forever()




