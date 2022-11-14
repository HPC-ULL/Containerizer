
from typing import List, Callable, Dict, TYPE_CHECKING

from pprint import pprint
from importlib_metadata import packages_distributions
import atexit
import platform
from dill.source import getsource, isfrommain, getimport

from dill.detect import getmodule

from colorama import Fore
from colorama import Style
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
import base64
from types import ModuleType

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


BUCKET_PATH = ".kubetmp"


"""
Usos:

-Entrenamientos de machine learning
-Paralelizacion de ejecuciones en contenedores (en caso de que no se necesite memoria compartida)
-Aprovechar de manera fácil la potencia de un clúster de Kubernetes
-Procesamiento de imágenes
-Alternativa a threads

"""

def containerize(join=True,*args, **kwargs, ):

    globals = inspect.currentframe().f_back.f_globals
    def decorator(func):
        def wrapper(*func_args, **func_kwargs):
            print(func)
            x = Containerize(func,args=func_args,*args,**kwargs, globals = globals)
            x.start()
            if(join):
                return x.join()
            else:
                return x
                
        return wrapper
    return decorator



class Containerize():

    def __init__(
        self, 
        function: Callable, 
        args: List = [],
        image : str = None,
        installs: List = [], 
        bysource: List = [], 
        extra_deps: Dict = {}, 
        extra_imports: List = [], 
        dependencies=None, 
        recurse_dependencies: bool = True, 
        minio_ip: str = None, 
        access_key: str = None, 
        secret_key: str = None, 
        globals: Dict = None):

        self.image = image

        if (dependencies is None):
            if (globals is None):
                globals = inspect.currentframe().f_back.f_globals

            self.dependencies = self.get_dependencies(
                function, globals, recurse=recurse_dependencies)
        else:
            self.dependencies = {
                "imports": set(), "sources": set(), "installs": set(), "deps": {}}

        self.dependencies["installs"].update(installs)
        self.dependencies["sources"].update(bysource)
        self.dependencies["imports"].update(extra_imports)

        for name, dep in extra_deps.items():
            self.dependencies["deps"][name] = dep

        self.function = function
        self.args = args

        self.copy_files = dependencies

        self.registry_ip = "localhost:31320"

        minio_ip = "localhost:9000"

        self.dockerClient = docker.from_env()

        self.id = str(uuid.uuid4())[:8]

        self.tmpFolder = "tmp"

        if (not os.path.exists(self.tmpFolder)):
            os.mkdir(self.tmpFolder)

        self.kuberesources = None

        self.namespace = "argo"

        self.podName = None

        config.load_kube_config()
        self.kubeApi = client.CoreV1Api()

        if not minio_ip:
            minio_ip = self.kubeApi.read_namespaced_service(
                "minio", self.namespace).status.load_balancer.ingress[0].ip + ":9000"

        artifactsConfig = yaml.safe_load(self.kubeApi.read_namespaced_config_map(
            "artifact-repositories", "argo").data["default-v1"])["s3"]

        if not access_key:
            access_key = base64.b64decode(self.kubeApi.read_namespaced_secret(
                artifactsConfig["accessKeySecret"]["name"], self.namespace).data[artifactsConfig["accessKeySecret"]["key"]]).decode("utf-8")
        if not secret_key:
            secret_key = base64.b64decode(self.kubeApi.read_namespaced_secret(
                artifactsConfig["secretKeySecret"]["name"], self.namespace).data[artifactsConfig["secretKeySecret"]["key"]]).decode("utf-8")

        self.minioclient = Minio(
            minio_ip,
            access_key=access_key,
            secret_key=secret_key,
            secure=not artifactsConfig["insecure"]
        )

        self.access_key = access_key
        self.secret_key = secret_key

        self.bucket = artifactsConfig["bucket"]

        if not self.minioclient.bucket_exists(self.bucket):
            self.minioclient.make_bucket(self.bucket)

        atexit.register(lambda: self.delete_files(f"{BUCKET_PATH}/{self.id}/"))

    def get_object_var_names(
        self, 
        obj ) -> List[str]:

        names = set()
        if (inspect.isbuiltin(obj)):
            return names
        if (inspect.isfunction(obj)):
            names = obj.__code__.co_names
        else:

            try:
                buf = StringIO(getsource(obj, lstrip=True))
            except TypeError:
                return names

            tokens = generate_tokens(buf.readline)
            for token in tokens:
                if (token.type == 1):
                    if (token.string is not None):
                        names.add(token.string)

        return names

    def get_dependencies(
            self, 
            obj, 
            globals, 
            recurse=True):

        dependencies = {"imports": set(), "sources": set(),
                        "installs": set(), "deps": {}}

        self.get_dependencies_recurse(obj, dependencies, globals, recurse)

        print("Dependencies:")
        pprint(dependencies)

        return dependencies

    def get_dependencies_recurse(
        self, 
        obj, 
        dependencies,
        globals, 
        recurse=True):

        names = self.get_object_var_names(obj)

        for name in names:

            dep = globals.get(name, None)
         
            if (dep != containerize and dep is not None and dependencies["deps"].get(name, None) is None):
                module = getmodule(dep)
                if (not isfrommain(dep)):
                    if (not isinstance(dep, (int, str, bool))):
                        dependencies["imports"].add(
                            getimport(dep)[:-1] + (" " if module is None else f" as {name}"))

                if (not inspect.isbuiltin(dep)):

                    dependencies["deps"][name] = dep

                    if (isfrommain(dep.__class__)):
                        dependencies["sources"].add(
                            getsource(dep.__class__, lstrip=True))

                    elif (module is not None):
                        pkg = packages_distributions().get(
                            module.__name__.split('.')[0], None)

                        if (pkg is not None):
                            dependencies["installs"].update(pkg)

                    elif (recurse):

                        self.get_dependencies_recurse(dep, dependencies, globals)

        return dependencies


    def image_exist(
        self, 
        imageName):

        try:
            self.dockerClient.images.pull(imageName)
            return True
        except docker.errors.NotFound:
            return False

    def get_image(
        self, 
        installs, 
        dependencies=[]):

        dependencies.sort()

        imageName = f"""{self.registry_ip}/containerizeri{"_".join(sorted(list(map(str,installs))))}d{"_".join(sorted(list(map(str,dependencies))))}:{platform.python_version()}"""

        if (self.image_exist(imageName)):
            print(f"La imagen '{imageName}' ya está presente en el registro")
            return imageName

        dockerFileTemplate = f"""
        FROM python:{platform.python_version()}
        
        RUN pip install dill
        RUN pip install minio
        
        """
        if (len(installs) > 0):
            dockerFileTemplate += f"RUN pip install {' '.join(map(str,installs))}"

        for dep in dependencies:
            if (isinstance(dep, ModuleType)):
                print("module")
            else:
                dockerFileTemplate += fr"COPY {dep} /usr/local/lib/python3.9/site-packages/{dep}"

        dockerFilePath = "."

        with open(os.path.join(dockerFilePath, "Dockerfile"), "w") as file:
            file.write(dockerFileTemplate)

        print(f"Creating image {imageName}")
        print(f"Dockerfile:\n{dockerFileTemplate}")
        self.dockerClient.images.build(path=dockerFilePath, tag=imageName)

        os.remove(os.path.join(dockerFilePath, "Dockerfile"))

        print(f"Pushing image {imageName}")
        self.dockerClient.images.push(imageName)

        return imageName

    def delete_files(
        self, 
        prefix):
        objects_to_delete = self.minioclient.list_objects(
            self.bucket, prefix=prefix, recursive=True)
        for obj in objects_to_delete:
            self.minioclient.remove_object(self.bucket, obj.object_name)
        #print(f"Artifacts deleted from {prefix}")

    def upload_variable(
        self, 
        var, 
        name, 
        prefix=""):
        with open(f'{self.tmpFolder}/{name}.tmp', 'wb') as handle:
            pickle.dump(var, handle, recurse=True,
                        byref=False, fmode="FILE_FMODE")

        if (prefix != ""):
            prefix += "/"

        self.minioclient.fput_object(
            self.bucket, f"{BUCKET_PATH}/{self.id}/{prefix}{name}", f'{self.tmpFolder}/{name}.tmp',
        )

        os.remove(f'{self.tmpFolder}/{name}.tmp')

    def download_variable(
        self, 
        name, 
        prefix=""):

        if (prefix != ""):
            prefix += "/"

        self.minioclient.fget_object(
            self.bucket, f"{BUCKET_PATH}/{self.id}/{prefix}{name}", f"{self.tmpFolder}/{name}.tmp")

        with open(f"{self.tmpFolder}/{name}.tmp", "rb") as outfile:
            var = pickle.load(outfile)

        os.remove(f"{self.tmpFolder}/{name}.tmp")

        return var

    def get_output(self):

        if (not self.finished):
            raise ValueError(
                "Container must have finished execution before getting output")

        output = self.download_variable("output")
        return output

    def join(self):
        if (self.podName == None):
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

        self.stream_pod(self.podName)

        done = False
        while(done == False):
            try:
                resp = self.kubeApi.read_namespaced_pod(name=self.podName,
                                                    namespace=self.namespace).status
                start = resp.container_statuses[0].state.terminated.started_at
                finish = resp.container_statuses[0].state.terminated.finished_at
                print(f"Pod '{self.podName}' ha finalizado en {finish-start}")
                done = True
            except:
                pass
                                            

        self.delete_files(f"{BUCKET_PATH}/{self.id}/tmp")
        self.finished = True

    def dependencie_upload(self, name):
        if (len(self.dependencies[name]) > 0):

            self.upload_variable(self.dependencies[name], name,  prefix="tmp")

            return f"""
minioclient.fget_object('{self.bucket}', '{BUCKET_PATH}/{self.id}/tmp/{name}', '/tmp/{name}')
with open(\'/tmp/{name}\', \'rb\') as input_file:
    {name} = pickle.load(input_file)

os.remove('/tmp/{name}')

for name, dep in {name}.items():
    main_module.__dict__[name] =  dep
"""
        else:
            return ""

    def start(self):

        if(self.image == None):
            self.image = self.get_image(self.dependencies["installs"])


        function_source = getsource(self.function,lstrip=True)
        if(function_source[0] == "@"):
            import re
            function_source = re.sub(r"@containerize(.*)\n", "", function_source)


        #self.uploadVariable(args, "args", prefix="tmp")

        nl = "\n"

        code = f"""
import dill as pickle

from minio import Minio

import os

minioclient = Minio(
            'minio:9000',
            access_key='{self.access_key}',
            secret_key='{self.secret_key}',
            secure=False
)

import __main__ as main_module


{nl.join(map(str,self.dependencies["imports"]))}

{nl.join(map(str,self.dependencies['sources']))}
"""

        code += self.dependencie_upload("deps")

        # Get variables
        code += f"""

#Arguments
minioclient.fget_object('{self.bucket}', '{BUCKET_PATH}/{self.id}/tmp/args', '/tmp/args')
with open(\'/tmp/args\', \'rb\') as input_file:
    args = pickle.load(input_file)


os.remove('/tmp/args')

#Function
{function_source}

#Call Function
output = {self.function.__name__}(*args)


with open('/tmp/out', \'wb\') as handle:
    pickle.dump(output, handle)


minioclient.fput_object(
            '{self.bucket}', '{BUCKET_PATH}/{self.id}/output', '/tmp/out',
)


"""
        command = ["python3", "-c", code]
        container = client.V1Container(
            name=f"{self.id}",
            image=self.image,
            command=command,
            resources=client.V1ResourceRequirements(limits=self.kuberesources)
        )

        spec = client.V1PodSpec(restart_policy="Never", containers=[container])

        podName = f"execution-{self.function.__name__.replace('_','')}-{self.id}"

        body = client.V1Job(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(name=podName),
            spec=spec
        )

        api_response = self.kubeApi.create_namespaced_pod(
            body=body,
            namespace=self.namespace)

        print("\nLanzado el Pod: '" + podName + "'")

        self.podName = podName
        self.upload_variable(self.args, "args", prefix="tmp")

        #self.uploadVariable(podName, args, "args")

        return api_response

    def stream_pod(
        self, 
        podName, 
        stdout=True, 
        stderr=True):

        w = watch.Watch()
        for e in w.stream(self.kubeApi.read_namespaced_pod_log, name=podName, namespace=self.namespace,):
            print(f"{Fore.GREEN}{podName}: {Fore.CYAN}{e}{Style.RESET_ALL}")

        w.stop()
