# Containerizer

Containerizer is a tool to run python functions in kubernetes containers

## Installation
Deploy the Kubernetes objects:
```bash
kubectl apply -f yamls/*
```


Use the package manager [pip](https://pip.pypa.io/en/stable/) to install Containerizer.

```bash
pip install pip install git+https://github.com/HPC-ULL/KubePipe
```

## Usage

```python
from containerizer.containerizer import Containerize


def printtext(args):
    pass

#Prints "hello world"
x = Containerize(printtext, args=["Hola mundo"])
x.start()
print(x.join())


```


## License

[MIT](https://choosealicense.com/licenses/mit/)