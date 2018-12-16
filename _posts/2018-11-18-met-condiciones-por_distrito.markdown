---
layout:     post
title:      Número de accidentes según condiciones meteorológicas o de la via (seleccionando el distrito)
tags: 		Spark AWS Tiempo Via Distritos
subtitle:  	Muestra el número de accidentes segun las circunstancias meteorológicas o de la via en las que se producieron, seleccionado el distrito a analizar
category:  project1
---
<!-- Start Writing Below in Markdown -->

# Descripción del Programa
Este programa muestra el número de accidentes producidos en virtud a las condiciones meteorológicas o de la via que había. Además, podemos seleccionar el distrito a analizar. 
Por tanto, se mostraría la situación en la que se produjo el accidente y el número de estos.

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

Para saber los distritos disponibles en los datos o la forma en la que se escriben, recomendamos la ejecución del programa inline `show_districts.py` o su descripción en [esta web][2]
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

spark-submit met_conditions_by_district.py (file) (distrito)

{% endhighlight %}

- El contenido ***file*** representa el archivo que queremos procesar y es importante que el formato de este archivo sea **csv**.
- La variable ***distrito*** es necesario especificarla. Para saber los que estan disponibles, recomendamos ir a la sección de **Descripción del programa** en esta misma página.


Si estamos probando el programa dentro de un **cluster**, podemos definir el numero de ***workers*** y de ***worker-nodes***. Para ello tendremos que añadir estos parametros a la linea de comando anterior:

{% highlight shell %}

--num-executors (number) --executor-cores (number)

{% endhighlight %}


# Salida del programa

{% highlight python %}

MONCLOA-ARAVACA

+--------------------------------------------------------+-------------------+
|Situation                                               |Number of accidents|
+--------------------------------------------------------+-------------------+
|Condiciones M Seco y Despejado                          |11441              |
|Condiciones de la Via: Siniestro en via seca y despejada|10986              |
|Condiciones de la Via: Mojada                           |2139               |
|Condiciones de la Via: Derrape por hielo                |65                 |
|Condiciones de la Via: Via con grava                    |57                 |
|Condiciones Meteorologicas: Niebla                      |44                 |
|Condiciones de la Via: Derrape por aceite               |38                 |
|Condiciones Meteorologicas: Hielo                       |37                 |
|Condiciones Meteorologicas: Nieve                       |35                 |
|Condiciones de la Via: Derrape por barro                |12                 |
|Condiciones Meteorologicas: Granizo                     |2                  |
+--------------------------------------------------------+-------------------+

{% endhighlight %}

[1]:https://artuyero.github.io/Cloud_BigData_UCM//about/
[2]:https://artuyero.github.io/Cloud_BigData_UCM//project1/2018/11/22/Show-Districts/


