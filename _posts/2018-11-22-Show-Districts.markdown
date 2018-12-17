---
layout:     post
title:      Muestra todos los distritos disponibles
tags: 		Spark AWS Distritos 	
category:  project1
---
<!-- Start Writing Below in Markdown -->

# Descripción del Programa
Este programa muestra todos los distritos disponibles en la fuente de datos utilizada

# Como ejecutarlo

## 1. Modo Local
Para ejecutar este programa necesitamos tener instalado en nuestro ordenador **Spark en Modo Local**. Además, necesitariamos tener instalado el paquete de ***Pandas*** de Python, que se descarga de la siguiente manera:

{% highlight shell %}

sudo apt-get install python-pip
sudo pip install numpy
sudo pip install pandas

{% endhighlight %}

## 2. Modo Cluster
Para ejecutarlo en este modo necesitariamos un ***cluster*** en AWS (Amazon Web Services), utilizando el modulo EMR (***Elastic Map Reduce***). En cuanto a las especificaciones del ***cluster***, se encuentran detalladas en la pagina [descripción][1] de esta web.



Una vez que ya hemos elegido el modo de ejecución y revisado todos los elementos, nos disponemos a ejecutar el programa. Para ello, utilizaremos la siguiente linea de comando: 

{% highlight shell %}

spark-submit show_districts.py (file)

{% endhighlight %}

- El contenido ***file*** representa el archivo que queremos procesar y es importante que el formato de este archivo sea **csv**.


Si estamos probando el programa dentro de un **cluster**, podemos definir el numero de ***workers*** y de ***worker-nodes***. Para ello tendremos que añadir estos parametros a la linea de comando anterior:

{% highlight shell %}

--num-executors (number) --executor-cores (number)

{% endhighlight %}


# Salida del programa

{% highlight python %}

+--------------------+
|      DISTRITO      |
+--------------------+
|ARGANZUELA       ...|
|BARAJAS          ...|
|CARABANCHEL      ...|
|CENTRO           ...|
|CHAMARTIN        ...|
|CHAMBERI         ...|
|CIUDAD LINEAL    ...|
|FUENCARRAL-EL PAR...|
|HORTALEZA        ...|
|LATINA           ...|
|MONCLOA-ARAVACA  ...|
|MORATALAZ        ...|
|PUENTE DE VALLECA...|
|RETIRO           ...|
|SALAMANCA        ...|
|SAN BLAS         ...|
|TETUAN           ...|
|USERA            ...|
|VICALVARO        ...|
|VILLA DE VALLECAS...|
|VILLAVERDE       ...|
+--------------------+


{% endhighlight %}

[1]:https://artuyero.github.io/Cloud_BigData_UCM//about/