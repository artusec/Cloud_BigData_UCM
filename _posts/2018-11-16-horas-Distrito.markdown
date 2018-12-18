---
layout:     post
title:      El rango horario con más accidentes (según el distrito y el día)
tags: 		Spark Hora Dia Distrito
subtitle:  	Muestra las horas donde se producen mayor cantidad de accidentes en base a un distrito y dia introducidos
category:  project1
---
<!-- Start Writing Below in Markdown -->

# Descripción del Programa
Este programa permite obtener la franja horaria con mayor accidentalidad, indicando un dia y distrito determinados. Además de mostrar las franjas horarias, muestra el número de accidentes y porcentaje que esto supone en el número total, en cada una.
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

spark-submit horasDistrito.py (file) (distrito) (dia de la semana)

{% endhighlight %}

- El contenido ***file*** representa el archivo que queremos procesar y es importante que el formato de este archivo sea **csv**.
- En el campo de ***dia de la semana***, hay que incluir el dia de la semana que queremos analizar (lunes, martes,etc.). **IMPORTANTE:** para que funcione el dia tiene que estar escrito en español
- La variable ***distrito*** es necesario especificarla. Para saber los que estan disponibles, recomendamos ir a la sección de **Descripción del programa** en esta misma página.


Si estamos probando el programa dentro de un **cluster**, podemos definir el numero de ***workers*** y de ***worker-nodes***. Para ello tendremos que añadir estos parametros a la linea de comando anterior:

{% highlight shell %}

--num-executors (number) --executor-cores (number)

{% endhighlight %}


# Salida del programa

{% highlight python %}

20    DE 6:00 A 6:59    169  6.966200
5   DE 14:00 A 14:59    163  6.718879
8   DE 17:00 A 17:59    138  5.688376
4   DE 13:00 A 13:59    132  5.441055
18    DE 4:00 A 4:59    131  5.399835
0   DE 00:00 A 00:59    127  5.234955
10  DE 19:00 A 19:59    123  5.070074
6   DE 15:00 A 15:59    122  5.028854
16    DE 2:00 A 2:59    117  4.822754
12  DE 20:00 A 20:59    113  4.657873
13  DE 21:00 A 21:59    112  4.616653
3   DE 12:00 A 12:59    102  4.204452
9   DE 18:00 A 18:59     95  3.915911
7   DE 16:00 A 16:59     95  3.915911
19    DE 5:00 A 5:59     95  3.915911
11    DE 1:00 A 1:59     94  3.874691
21    DE 7:00 A 7:59     83  3.421270
17    DE 3:00 A 3:59     82  3.380049
14  DE 22:00 A 22:59     76  3.132729
15  DE 23:00 A 23:59     71  2.926628
22    DE 8:00 A 8:59     61  2.514427
1   DE 10:00 A 10:59     43  1.772465
2   DE 11:00 A 11:59     42  1.731245
23    DE 9:00 A 9:59     40  1.648805

{% endhighlight %}


### Moncloa-Aravaca | Viernes

<img src="https://artuyero.github.io/Cloud_BigData_UCM/img/graphics/horasDistrito(Moncloa)(Viernes).png" height="150%" width="150%">

### Chamartin | Sábado

<img src="https://artuyero.github.io/Cloud_BigData_UCM/img/graphics/horasDistrito(Chamartin)(Sabado).png" height="150%" width="150%">


[1]:https://artuyero.github.io/Cloud_BigData_UCM//about/
[2]:https://artuyero.github.io/Cloud_BigData_UCM//project1/2018/11/22/Show-Districts/


