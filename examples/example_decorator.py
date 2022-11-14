from containerizer.containerizer import containerize


@containerize(minio_ip = "localhost:9000")
def printtext(args):
    
    print(args)
    return "output"
    

printtext("hola mundo\n" * 10 )

