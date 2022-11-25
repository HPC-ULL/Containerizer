

from sympy import isprime
from containerizer import containerize

@containerize(minio_ip = "localhost:9000")
def print_primes(n):
    for i in range(n):
        if(isprime(i)):
            print(i)

print_primes(100)


from sympy import isprime
from containerizer.containerizer import Containerize

def print_primes(n):
    for i in range(n):
        if(isprime(i)):
            print(i)

x = Containerize(print_primes, args = (100,))
x.start()
x.join()

print_primes(100)

