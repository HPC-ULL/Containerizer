

from time import sleep
import dill as dill
from containerizer.containerizer import Containerize

from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression


from sklearn.model_selection import train_test_split
from sklearn import datasets

iris = datasets.load_iris()

X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.2)


def train(X_train, X_test, y_train, y_test):

    pipe = make_pipeline(OneHotEncoder(handle_unknown='ignore'),LogisticRegression())

    print("Training pipeline")
    pipe.fit(X_train,y_train)

    print("Fit pipeline")
    print(pipe.score(X_test, y_test))


#x = Thread(target=thread_function, args=(1,))

containers = []
for i in range(10):
    containers.append(Containerize(train, args=(X_train, X_test, y_train, y_test)))


for container in containers:
    container.start()

for container in containers:
    container.join()





