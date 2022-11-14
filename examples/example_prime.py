from containerizer.containerizer import containerize

def is_prime(n: int) -> bool:
    if n <= 3:
        return n > 1
    if not n%2 or not n%3:
        return False
    i = 5
    stop = int(n**0.5)
    while i <= stop:
        if not n%i or not n%(i + 2):
            return False
        i += 6
    return True


@containerize(minio_ip = "localhost:9000")
def print_primes(n):
    for i in range(n):
        if(is_prime(i)):
            print(i)

    
print_primes(100)

