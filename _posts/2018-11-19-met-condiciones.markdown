---
layout:     post
title:      Número de accidentes en virtud a la situación meterológica o de la via en la que se producieron
tags: 		Spark AWS Dia Distritos  	
category:  project1
---
<!-- Start Writing Below in Markdown -->

# Descripción del Programa
Este programa muestra el número de accidentes producidos según diferentes situaciones ambientales (meterológicas o de la via). 

Las condiciones meterológicas pueden ser:
- Seco y Despejado
- Nieve
- Niebla
- Hielo
- Granizo

Las condiciones de la via pueden ser:
- Siniestro en via seca y despejada
- Via con grava
- Derrape por aceite
- Derrape por hielo
- Derrape por barro

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

spark-submit met_conditions.py (file)

{% endhighlight %}

- El contenido ***file*** representa el archivo que queremos procesar y es importante que el formato de este archivo sea **csv**.


Si estamos probando el programa dentro de un **cluster**, podemos definir el numero de ***workers*** y de ***worker-nodes***. Para ello tendremos que añadir estos parametros a la linea de comando anterior:

{% highlight shell %}

--num-executors (number) --executor-cores (number)

{% endhighlight %}


# Salida del programa

{% highlight python %}

+--------------------------------------------------------+-------------------+
|Situation                                               |Number of accidents|
+--------------------------------------------------------+-------------------+
|Condiciones Meteorologicas: Seco y Despejado            |219675             |
|Condiciones de la Via: Siniestro en via seca y despejada|214315             |
|Condiciones de la Via: Mojada                           |31537              |
|Condiciones de la Via: Via con grava                    |611                |
|Condiciones de la Via: Derrape por aceite               |560                |
|Condiciones de la Via: Derrape por hielo                |359                |
|Condiciones Meteorologicas: Nieve                       |353                |
|Condiciones Meteorologicas: Niebla                      |353                |
|Condiciones Meteorologicas: Hielo                       |210                |
|Condiciones de la Via: Derrape por barro                |192                |
|Condiciones Meteorologicas: Granizo                     |34                 |
+--------------------------------------------------------+-------------------+

{% endhighlight %}

[1]:https://artuyero.github.io/Cloud_BigData_UCM//about/