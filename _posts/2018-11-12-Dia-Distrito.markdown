---
layout:     post
title:      Distritos con más accidentes (según el dia de la semana)
tags: 		Spark AWS Dia Distritos 
subtitle:  	Muestra los distritos con más accidentes, segun el día introducido
category:  project1
---
<!-- Start Writing Below in Markdown -->

# Descripción del Programa
Este programa permite obtener los distritos con mayor accidentalidad, según el dia que especifiquemos. Además, muestra el numero de accidentes por cada distrito. 
Para saber los distritos disponibles en los datos o la forma en la que se escriben, recomendamos la ejecución del programa `show_districts.py` o su descripción en [esta web][2]
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

spark-submit dia_distrito.py (file) (dia de la semana)

{% endhighlight %}

- El contenido ***file*** representa el archivo que queremos procesar y es importante que el formato de este archivo sea **csv**.
- En el campo de ***dia de la semana***, hay que incluir el dia de la semana que queremos analizar (lunes, martes,etc.). **IMPORTANTE:** para que funcione el dia tiene que estar escrito en español


Si estamos probando el programa dentro de un **cluster**, podemos definir el numero de ***workers*** y de ***worker-nodes***. Para ello tendremos que añadir estos parametros a la linea de comando anterior:

{% highlight shell %}

--num-executors (number) --executor-cores (number)

{% endhighlight %}


# Salida del programa

{% highlight python %}

DISTRITO  COUNT
SALAMANCA   2670
CHAMARTIN   2558
CIUDAD LINEAL   2463
PUENTE DE VALLECAS   2456
CENTRO   2291
CARABANCHEL   2067
CHAMBERI   2002
FUENCARRAL-EL PARDO   1976
MONCLOA-ARAVACA   1860
SAN BLAS   1856
RETIRO   1852
TETUAN   1769
ARGANZUELA   1744
LATINA   1704
HORTALEZA   1417
USERA   1151
VILLAVERDE    970
MORATALAZ    966
VILLA DE VALLECAS    792
BARAJAS    573
VICALVARO    399

{% endhighlight %}


### Lunes

<img src="https://artuyero.github.io/Cloud_BigData_UCM/img/graphics/dia_distrito(lunes).png" height="150%" width="150%">

### Sábado

<img src="https://artuyero.github.io/Cloud_BigData_UCM/img/graphics/dia_distrito(sabado).png" height="150%" width="150%">


[1]:https://artuyero.github.io/Cloud_BigData_UCM//about/
[2]:https://artuyero.github.io/Cloud_BigData_UCM//project1/2018/11/22/Show-Districts/