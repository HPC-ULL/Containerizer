from containerizer import Containerizer


def printtext(args):
    print(args)
    
x = Containerizer(printtext, args=["Hola mundo\n" * 10], minio_ip = "localhost:9000", image = "python:3.9.13")

x.start()


print(x.join())


