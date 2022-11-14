from containerizer.containerizer import Containerize


def printtext(args):
    pass
    
x = Containerize(printtext, args=["Hola mundo\n" * 10], minio_ip = "localhost:9000", image = "localhost:31320/containerizerid:3.9.13")

x.start()


print(x.join())


