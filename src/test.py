import pyspark, pathlib

p=pathlib.Path(pyspark.__file__).parent
print('PySpark:',pyspark.__version__) 
print('JAR directory:',p/'jars') 
print('\n'.join(x.name for x in (p/'jars').glob('hadoop-*.jar')))