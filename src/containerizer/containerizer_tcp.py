from dill.source import getsource, isfrommain, getimport

#from inspect import getsource

import inspect
import os
from time import sleep
import yaml
import dill as pickle

from tokenize import generate_tokens
from io import StringIO

import uuid

import docker
from minio import Minio

from kubernetes import client, config, watch
from kubernetes.stream import stream
from kubernetes.watch import watch
from types import ModuleType

from threading import Thread

import socket
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import platform
import uuid
import atexit

from importlib_metadata import packages_distributions

import __main__ as main_module

from pprint import pprint
from kubernetes.client.rest import ApiException

from tempfile import TemporaryFile


BUCKET_PATH = ".kubetmp"

from io import BytesIO



"""
Usos:

-Entrenamientos de machine learning
-Paralelizacion de ejecuciones en contenedores (en caso de que no se necesite memoria compartida)
-Aprovechar de manera fácil la potencia de un clúster de Kubernetes
-Procesamiento de imágenes
-Alternativa a threads

"""


class Containerize():

    def __init__(self,function, args=[], installs = [], bysource = [],dependencies = None, recurseDependencies = True, minio_ip = None, access_key = None, secret_key = None, globals = None):

        if(dependencies is None):
            if(globals is None):
                globals = inspect.currentframe().f_back.f_globals
                
            self.dependencies = self.getDependencies(function,globals, recurse = recurseDependencies)
        else:
            self.dependencies = {  "imports" :set(), "sources" : set(), "installs" : set(), "deps" : {}}

        self.dependencies["installs"].update(installs)
        self.dependencies["sources"].update(bysource)

        self.delim = b'@@~$'
        self.bufferSize = 1024
        self.currentPort = None

        

        self.function = function
        self.args = args

        self.copy_files = dependencies

        self.registry_ip = "localhost:31320"


        self.dockerClient = docker.from_env()

        self.id = str(uuid.uuid4())[:8]

        self.tmpFolder = "tmp"

        if(not os.path.exists(self.tmpFolder)):
            os.mkdir(self.tmpFolder)

        self.kuberesources = None

        self.namespace = "argo"

        self.podName = None

        config.load_kube_config()
        self.kubeApi = client.CoreV1Api()

     

    def getObjectVarNames(self,obj):
        names = []
        if(inspect.isbuiltin(obj)):
            return names
        if(inspect.isfunction(obj)):
            names = obj.__code__.co_names
        else:
            try:
                buf = StringIO(getsource(obj,lstrip=True))
            except TypeError:
                return names

            tokens = generate_tokens(buf.readline)
            for token in tokens:
                if(token.type == 1):
                    if(token.string is not None):
                            names.append(token.string)

        return names

    def getDependencies(self,obj, globals, recurse = True):

        dependencies = {"imports" :set(), "sources" : set(), "installs" : set(), "deps" : {}}

        self.getDependenciesRecurse(obj, dependencies, globals, recurse)


        print("Dependencies:")
        pprint(dependencies)

        return dependencies
        

        

    def getDependenciesRecurse(self,obj, dependencies, globals, recurse = True):

            names =  self.getObjectVarNames(obj)

            for name in names:

                dep = globals.get(name,None)
                if(dep is not None and dependencies["deps"].get(name,None) is None):
                    

                    if(not isfrommain(dep)):
                        dependencies["imports"].add(getimport(dep))

                    module = inspect.getmodule(dep)

                    if(not inspect.isbuiltin(dep) and module is not None):

                        dependencies["deps"][name] = dep

                        pkg = packages_distributions().get(inspect.getmodule(dep).__name__.split('.')[0],None)
                        if(pkg is not None):
                            dependencies["installs"].update(pkg)


                        if(recurse and pkg is None):
                            if(isfrommain(dep.__class__)):
                                dependencies["sources"].add(getsource(dep.__class__,lstrip=True))

                            self.getDependenciesRecurse(dep, dependencies, globals)
                    

            return dependencies

   



    def imageExist(self,imageName):

        try:
            self.dockerClient.images.pull(imageName)
            return True
        except docker.errors.NotFound:
            return False


    def getImage(self, installs, dependencies = []):

        dependencies.sort()

        imageName = f"""{self.registry_ip}/containerizeri{"_".join(map(str,installs))}d{"_".join(map(str,dependencies))}:{platform.python_version()}"""

        if(self.imageExist(imageName)):
            print(f"La imagen '{imageName}' ya está presente en el registro")
            return imageName

        dockerFileTemplate = f"""
        FROM python:{platform.python_version()}
        
        RUN pip install dill
        
        """
        if(len(installs ) > 0):
            dockerFileTemplate+=f"RUN pip install {' '.join(map(str,installs))}"

        for dep in dependencies:
            if(isinstance(dep, ModuleType)):
                print("module")
            else:
                dockerFileTemplate+=fr"COPY {dep} /usr/local/lib/python3.9/site-packages/{dep}"
            

        dockerFilePath = r"C:\Users\danis\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.9_qbz5n2kfra8p0\LocalCache\local-packages\Python39\site-packages"

        with open(os.path.join(dockerFilePath,"Dockerfile"), "w") as file:
            file.write(dockerFileTemplate)
    
        print(f"Creating image {imageName}")
        print(f"Dockerfile:\n{dockerFileTemplate}")
        self.dockerClient.images.build(path=dockerFilePath, tag = imageName)

        os.remove(os.path.join(dockerFilePath,"Dockerfile"))

        print(f"Pushing image {imageName}")
        self.dockerClient.images.push(imageName)

        return imageName



    def getOutput(self):

        if(not self.finished):
            raise ValueError("Container must have finished execution before getting output")

        if(not self.sendoutput):
            raise ValueError("Select option 'sendoutput' when starting the container to be able to retrieve the output")
            
        output = self.receiveOutput(self.host, self.port)
        return output

    def join(self):
        if( self.podName == None):
            raise ValueError("Container must be started before join")

        while True:
            resp = self.kubeApi.read_namespaced_pod(name=self.podName,
                                                    namespace=self.namespace)
            if resp.status.phase != 'Pending':
                break

            if resp.status.phase == 'Failed':
                print(f"Pod '{self.podName}' failed, aborting...")
                return

            sleep(1)

        x = Thread(target=self.streamPod, args= (self.podName, True,True))
        x.start()

        out = None

        if(self.sendoutput):
            out =  self.receiveOutput(self.host, self.port)
        
        x.join()

        self.finished = True

        return out



    def sendVars(self, host,port, x, y , pipe):

        s = self.createSocket(host, port)

        data = pickle.dumps(x, byref=False) + self.delim + pickle.dumps(y,byref=False) + self.delim + pickle.dumps(pipe,byref=False) 

        s.sendall(data)

        s.close()

    def receiveOutput(self,host, port):
        s = self.createSocket(host, port)

        data = b""
        while(True):
            bytes_read = s.recv(self.bufferSize)
            if not bytes_read:
                break
            
            data+=bytes_read

        s.close()        
        if(data == b""):
            raise Exception("Output is empty")

        return pickle.loads(data)

    def getLbIP(self,serviceName):
        api_response = self.kubeApi.read_namespaced_service_status(    
            name=serviceName,
            namespace=self.namespace)


        # while(api_response.status.load_balancer.ingress == None):
        #     api_response = self.kubeApi.read_namespaced_service_status(    
        #     name=serviceName,
        #     namespace=self.namespace)

        # host = api_response.status.load_balancer.ingress[0].ip
        # if host == None: host = "localhost"


        return "localhost" , api_response.spec.ports[0].node_port

    def create_tcp_service(self,type,selector,serviceName, nodePort=None):

            servicespec=client.V1ServiceSpec(
                    type = type,
                    selector=selector,
                    ports=[client.V1ServicePort(
                        port=3000,
                        target_port=3000,
                        protocol="TCP",
                        node_port=nodePort
                )]
            )

            servicebody = client.V1Service(

                api_version="v1",
                kind="Service",
                metadata=client.V1ObjectMeta(
                    name=serviceName,
                    labels={ "app" : "kubepipe"}
                ),
                spec = servicespec              
            )

            try:
                self.kubeApi.create_namespaced_service(
                    namespace = self.namespace,
                    body = servicebody
                )
            except ApiException as e:
                if(e.status == 422):
                    raise ValueError("NodePort already in use")

            print("\nLanzado el servicio: '" + serviceName + "'")


    def start(self, sendoutput = True):

        self.sendoutput = sendoutput

        imageName = self.getImage(self.dependencies["installs"])

        resources = self.kuberesources

        function = self.function
        

        nl = "\n"
        code = f"""

import socket

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 3000
              
BUFFER_SIZE = {self.bufferSize}


delim = {self.delim}

print("Initialize socket")
s = socket.socket()
s.bind((SERVER_HOST, SERVER_PORT))
s.listen(1)
print("Socket Listening")



import __main__ as main_module



import dill as pickle


import os

{nl.join(map(str,self.dependencies["imports"]))}

{nl.join(map(str,self.dependencies['sources']))}

def connect():

    while True:
        try:

            client_socket, address = s.accept()
            client_socket.settimeout(1.0)

            print("Connected " + str(address))

            print("Ping host")
            client_socket.send(b"p")

            a = client_socket.recv(1)

            if(a == b"p"):
                print("Reverse ping host")
                client_socket.settimeout(1.0)
                return client_socket

            else:
                print("Not connected")

        except Exception as e:
            pass


def receiveVars():

    s = connect()

    output = []
    data = b""

    while True:
        bytes_read = s.recv(BUFFER_SIZE)
        if not bytes_read:
            break

        data+=bytes_read

    for var in data.split(delim):
        output.append(pickle.loads(var))

    s.close()

    return output


def sendOutPut(out):

    s = connect()

    s.sendall(pickle.dumps(out))

    s.close()


print("Receiving variables")
args, vars = receiveVars()

main_module.__dict__.update(vars)

print("Variables received")

#Function
{getsource(function,lstrip=True)}

#Call Function
output = {function.__name__}(*args)

if({sendoutput}):
    sendOutPut(output)

s.close()

"""

        command = ["python3" , "-u", "-c", code]
        container = client.V1Container(
            name=f"{self.id}",
            image=imageName,
            command=command,
            resources = client.V1ResourceRequirements(limits=resources)
        )

        spec=client.V1PodSpec(restart_policy="Never", containers=[container])

        podName = f"execution-{function.__name__}-{self.id}"

        body = client.V1Job(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(name=podName, labels = {"pod": podName, "app" : "containerizer"}),
            spec=spec
            )

        api_response = self.kubeApi.create_namespaced_pod(
            body=body,
            namespace=self.namespace)

        print("\nLanzado el Pod: '" + podName + "'")

        self.podName = podName
        self.serviceName = podName + "-serv"
        self.create_tcp_service("NodePort", {"pod" : podName}, self.serviceName)
        self.host, self.port = self.getLbIP(self.serviceName)
        
        self.sendVars(self.host,self.port, [self.args, self.dependencies["deps"]] )

        return api_response

    def getLbIP(self,serviceName):
        api_response = self.kubeApi.read_namespaced_service_status(    
            name=serviceName,
            namespace=self.namespace)



        return "localhost" , api_response.spec.ports[0].node_port


    def sendVars(self, host,port,vars):

        s = self.createSocket(host, port)

        data = b""
        for i in range(len(vars)-1):
            data+=pickle.dumps(vars[i], byref=False) + self.delim

        data+=pickle.dumps(vars[-1],byref=False)

        s.sendall(data)

        s.close()

    def streamPod(self,podName, stdout = True, stderr = True):

        w = watch.Watch()
        for e in w.stream(self.kubeApi.read_namespaced_pod_log, name=podName, namespace=self.namespace):
            print(f"{podName}: {e}")


    def receiveOutput(self,host, port):
        s = self.createSocket(host, port)

        data = b""
        while(True):
            bytes_read = s.recv(self.bufferSize)
            if not bytes_read:
                break
            
            data+=bytes_read

        s.close()        
        if(data == b""):
            raise Exception("Output is empty")

        return pickle.loads(data)
        
    def createSocket(self,host,port):

        while True:
            try:
                s = socket.create_connection(((host,port)))
                s.settimeout(1.0)
                ping = s.recv(1)
                if(ping != b"p"):
                    raise Exception("Not connected")

                s.send(b"p")

                s.settimeout(5)
                return s

            except Exception as e:
                sleep(0.1)

        

