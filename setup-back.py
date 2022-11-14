from distutils.core import setup
setup(
  name = 'containerizer',        
  packages = ['containerizer'],  
  version = '0.1.1',      
  license='MIT',       
  description = 'Tool to execute pieces of python code in kubernetes containers',   
  author = 'Daniel Suárez Labena',                 
  author_email = 'dsuarezl@ull.edu,es',    
  url = 'https://github.com/alu0101040882/kubernetes-ml-pipeline-TFM',  
  download_url = 'https://github.com/alu0101040882/kubernetes-ml-pipeline-TFM',   
  keywords = ['Kubernetes','Machine learning'],   
  install_requires=[           
        'kubernetes',
        'minio',
        'dill',
        'docker'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      
    'Intended Audience :: Developers',      
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   
    'Programming Language :: Python :: 3',      
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)